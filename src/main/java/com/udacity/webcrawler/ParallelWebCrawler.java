package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final PageParserFactory parserFactory;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          PageParserFactory parserFactory,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls
  ) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.parserFactory = parserFactory;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();

    for (String url : startingUrls) {
      pool.invoke(new CustomCrawlTask(maxDepth, deadline, url, counts, visitedUrls));
    }
    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  // TODO: Complete method
  public final class CustomCrawlTask extends RecursiveTask {
    private final int maxDepth;
    private final Instant deadline;
    private final String url;
    private final ConcurrentHashMap<String, Integer> counts;
    private final ConcurrentSkipListSet<String> visitedUrls;

    public CustomCrawlTask(
            int maxDepth,
            Instant deadline,
            String url,
            ConcurrentHashMap<String, Integer> counts,
            ConcurrentSkipListSet<String> visitedUrls
    ) {
      this.maxDepth = maxDepth;
      this.deadline = deadline;
      this.url = url;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected Map<String, Integer> compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return Collections.emptyMap();
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return Collections.emptyMap();
        }
      }
      if (!visitedUrls.add(url)) {
        return Collections.emptyMap();
      }
      PageParser.Result result = parserFactory.get(url).parse();

      result.getWordCounts().forEach((key, value) ->
              counts.merge(key, value, Integer::sum));

      List<CustomCrawlTask> customCrawlTask = result.getLinks().stream()
              .map(link -> new CustomCrawlTask(maxDepth - 1, deadline, link, counts, visitedUrls))
              .collect(Collectors.toList());
      invokeAll(customCrawlTask);

      return counts;
    }
  }
}