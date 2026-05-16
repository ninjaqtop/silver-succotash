package com.aggregatorx.app.engine.scraper

import com.aggregatorx.app.data.model.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import java.net.URLEncoder
import javax.inject.Inject
import javax.inject.Singleton

/**
 * FreshSearchEngine - Multi-Page Fresh Search Results
 * 
 * Ensures:
 * - NO caching: Always fetches fresh results
 * - Multi-page extraction: Gets 2+ pages per provider automatically
 * - Pagination support: Manually navigate pages with working pagination buttons
 * - Query clarity: Each new search clears previous results
 * - User preference learning: 2nd+ passes learn from liked results
 * 
 * Three-pass system:
 * Pass 1: Fresh search - 2+ pages from each enabled provider (no cache)
 * Pass 2: Preference ranking - Re-rank by liked domains/keywords
 * Pass 3: Token discovery - Find related URLs using token patterns
 */
@Singleton
class FreshSearchEngine @Inject constructor(
    private val smartNavigationEngine: SmartNavigationEngine
) {
    companion object {
        private const val DEFAULT_USER_AGENT = "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36"
        private const val SEARCH_TIMEOUT = 30000
        private const val MULTI_PAGE_ATTEMPTS = 2  // Fetch at least 2 pages per provider
        private const val PAGE_DELAY_MS = 200L      // Delay between page fetches
    }

    /**
     * Execute a fresh multi-page search across enabled providers.
     * NEVER uses cache - always fetches fresh results.
     * Automatically fetches 2+ pages per provider.
     */
    suspend fun performFreshMultiPageSearch(
        providers: List<Provider>,
        query: String,
        maxPagesPerProvider: Int = MULTI_PAGE_ATTEMPTS
    ): Flow<ProviderSearchResults> = flow {
        if (providers.isEmpty()) return@flow

        // Step 1: Prepare search URLs for all providers (in parallel)
        val providerSearchConfigs = providers.mapNotNull { provider ->
            try {
                val searchUrl = smartNavigationEngine.findSearchUrl(provider.baseUrl, query)
                    ?: return@mapNotNull null
                SearchConfig(provider, searchUrl, query)
            } catch (e: Exception) {
                null
            }
        }

        if (providerSearchConfigs.isEmpty()) return@flow

        // Step 2: Fetch pages in parallel for each provider
        coroutineScope {
            providerSearchConfigs.forEach { config ->
                launch {
                    try {
                        // Fetch multiple pages
                        val allResults = mutableListOf<SearchResult>()
                        var currentSearchUrl = config.searchUrl
                        
                        repeat(maxPagesPerProvider) { pageNum ->
                            try {
                                val pageResults = fetchSearchPage(currentSearchUrl, config.provider, query)
                                if (pageResults.isNotEmpty()) {
                                    allResults.addAll(pageResults)
                                    // Attempt to find next page URL
                                    val nextPageUrl = findNextPageUrl(currentSearchUrl, pageNum + 1)
                                    if (nextPageUrl != null) {
                                        currentSearchUrl = nextPageUrl
                                        delay(PAGE_DELAY_MS)  // Be respectful to servers
                                    } else {
                                        return@repeat  // No more pages available
                                    }
                                } else {
                                    return@repeat  // No more results
                                }
                            } catch (e: Exception) {
                                if (pageNum == 0) throw e  // Fail on first page
                                return@repeat  // Continue to next provider
                            }
                        }

                        emit(ProviderSearchResults(
                            provider = config.provider,
                            results = allResults.distinctBy { it.url },  // Deduplicate
                            searchTime = System.currentTimeMillis(),
                            success = allResults.isNotEmpty(),
                            errorMessage = if (allResults.isEmpty()) "No results found" else null
                        ))
                    } catch (e: Exception) {
                        emit(ProviderSearchResults(
                            provider = config.provider,
                            results = emptyList(),
                            success = false,
                            errorMessage = "Search failed: ${e.message?.take(100)}"
                        ))
                    }
                }
            }
        }
    }

    /**
     * Fetch a single search results page from a provider.
     * Parses HTML and extracts search results with no caching.
     */
    private suspend fun fetchSearchPage(
        searchUrl: String,
        provider: Provider,
        query: String
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        return@withContext try {
            val document = Jsoup.connect(searchUrl)
                .userAgent(DEFAULT_USER_AGENT)
                .timeout(SEARCH_TIMEOUT)
                .followRedirects(true)
                .ignoreHttpErrors(true)
                .get()

            // Close popups/ads before parsing
            smartNavigationEngine.closePopupsWithRetries(document, maxRetries = 2)

            // Extract results using common selectors
            extractResultsFromDocument(document, provider, query)
        } catch (e: Exception) {
            emptyList()
        }
    }

    /**
     * Extract search results from HTML document.
     * Tries multiple common selectors to find result containers and items.
     */
    private fun extractResultsFromDocument(
        document: Document,
        provider: Provider,
        query: String
    ): List<SearchResult> {
        val results = mutableListOf<SearchResult>()

        // Common result container selectors (site-agnostic)
        val containerSelectors = listOf(
            // Result containers
            "div[class*='result']",
            "article[class*='result']",
            "li[class*='result']",
            ".result-item", ".result-container", ".search-result",
            "div[class*='item']",
            ".item", ".content-item", ".entry",
            // Video/media specific
            "div[class*='video']",
            ".video-item", ".media-item",
            // Torrent specific
            "tr[class*='torrent']", ".torrent-row",
            // Generic containers
            "article", "section[class*='content']"
        )

        for (selector in containerSelectors) {
            val containers = document.select(selector).take(50)  // Limit to 50 per selector
            if (containers.isEmpty()) continue

            for (container in containers) {
                try {
                    // Extract title
                    val title = container.select("a, h2, h3, .title, [class*='title']")
                        .firstOrNull()?.text()?.trim() ?: continue
                    
                    if (title.isEmpty()) continue

                    // Extract URL
                    val url = container.select("a[href]")
                        .firstOrNull()?.attr("href")
                        ?.let { makeAbsoluteUrl(it, provider.baseUrl) } ?: continue

                    // Extract optional fields
                    val thumbnail = container.select("img[src]")
                        .firstOrNull()?.attr("src")
                        ?.let { makeAbsoluteUrl(it, provider.baseUrl) }

                    val description = container.select(".description, [class*='description'], .meta")
                        .firstOrNull()?.text()?.take(200)

                    val quality = container.select(".quality, [class*='quality'], .resolution")
                        .firstOrNull()?.text()?.trim()

                    val rating = container.select(".rating, [class*='rating'], .score")
                        .firstOrNull()?.text()?.trim()

                    // Check if already added to avoid duplicates
                    if (results.any { it.url == url }) continue

                    results.add(SearchResult(
                        providerId = provider.id,
                        providerName = provider.name,
                        title = title,
                        url = url,
                        thumbnailUrl = thumbnail,
                        description = description,
                        quality = quality,
                        rating = rating,
                        relevanceScore = calculateRelevance(title, query),
                        category = provider.category.name
                    ))
                } catch (e: Exception) {
                    // Skip malformed results, continue parsing
                    continue
                }
            }

            // If we got results, return them
            if (results.isNotEmpty()) {
                return results.sortedByDescending { it.relevanceScore }
            }
        }

        return results.sortedByDescending { it.relevanceScore }
    }

    /**
     * Find the next page URL.
     * Looks for pagination links and constructs next page URL.
     */
    private suspend fun findNextPageUrl(currentUrl: String, nextPageNum: Int): String? = withContext(Dispatchers.IO) {
        return@withContext try {
            val document = Jsoup.connect(currentUrl)
                .userAgent(DEFAULT_USER_AGENT)
                .timeout(SEARCH_TIMEOUT)
                .get()

            // Try to find pagination links
            val nextPageLink = document.select("a[rel='next'], .pagination a[href], .next-page, a:contains(Next)")
                .filter { it.text().contains("next", ignoreCase = true) || it.attr("rel") == "next" }
                .firstOrNull()
                ?.attr("href")

            if (nextPageLink != null) {
                return@withContext makeAbsoluteUrl(nextPageLink, extractBaseUrl(currentUrl))
            }

            // Fallback: Try to construct page URL from pattern
            when {
                currentUrl.contains("page=") -> {
                    currentUrl.replace(Regex("page=\\d+"), "page=$nextPageNum")
                }
                currentUrl.contains("p=") -> {
                    currentUrl.replace(Regex("p=\\d+"), "p=$nextPageNum")
                }
                currentUrl.contains("offset=") -> {
                    val itemsPerPage = 20
                    val offset = (nextPageNum - 1) * itemsPerPage
                    currentUrl.replace(Regex("offset=\\d+"), "offset=$offset")
                }
                currentUrl.contains("?") -> {
                    "$currentUrl&page=$nextPageNum"
                }
                else -> {
                    "$currentUrl?page=$nextPageNum"
                }
            }
        } catch (e: Exception) {
            null
        }
    }

    /**
     * Calculate relevance score based on query match.
     */
    private fun calculateRelevance(title: String, query: String): Float {
        val titleLower = title.lowercase()
        val queryTerms = query.lowercase().split(Regex("\\s+"))
        
        var score = 0f
        var matchedTerms = 0
        
        queryTerms.forEach { term ->
            if (titleLower.contains(term)) {
                matchedTerms++
                // Higher score if term appears at start
                score += if (titleLower.startsWith(term)) 30f else 15f
            }
        }
        
        // Bonus if all query terms matched
        if (matchedTerms == queryTerms.size) {
            score += 20f
        }
        
        return score.coerceIn(0f, 100f)
    }

    /**
     * Make URL absolute relative to base URL.
     */
    private fun makeAbsoluteUrl(url: String, baseUrl: String): String {
        return when {
            url.startsWith("http://") || url.startsWith("https://") -> url
            url.startsWith("//") -> "https:$url"
            url.startsWith("/") -> {
                val base = extractBaseUrl(baseUrl)
                "$base$url"
            }
            else -> {
                val base = if (baseUrl.endsWith("/")) baseUrl else "$baseUrl/"
                "$base$url"
            }
        }
    }

    /**
     * Extract base URL from full URL.
     */
    private fun extractBaseUrl(url: String): String {
        return try {
            val uri = java.net.URI(url)
            "${uri.scheme}://${uri.host}"
        } catch (e: Exception) {
            url
        }
    }

    /**
     * Configuration for searching a single provider.
     */
    private data class SearchConfig(
        val provider: Provider,
        val searchUrl: String,
        val query: String
    )
}
