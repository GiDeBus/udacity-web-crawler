package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.ArrayList;

import static java.util.concurrent.ForkJoinTask.invokeAll;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {

  private final Clock clock;
  private final PageParserFactory parserFactory;
  private final Duration timeout;
  private final int popularWordCount;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;

  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls,
      @TargetParallelism int threadCount
  ) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);

    Map<String, Integer> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = new ConcurrentSkipListSet<>();

    for (String url : startingUrls) {

      pool.invoke(new CustomConcurrentTask(url, deadline, maxDepth, counts, visitedUrls));
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
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
  private class CustomConcurrentTask extends RecursiveTask<Map<String, Integer>> {
    String url;
    Instant deadline;
    int maxDepth;
    Map<String, Integer> counts;
    Set<String> visitedUrls;

    private CustomConcurrentTask(
            String url,
            Instant deadline,
            int maxDepth,
            Map<String, Integer> counts,
            Set<String> visitedUrls
    ) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
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
      if (visitedUrls.contains(url)) {
        return Collections.emptyMap();
      }
      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();
      Map<String, Integer> localCounts = new HashMap<>();
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        localCounts.merge(e.getKey(), e.getValue(), Integer::sum);
      }
      List<CustomConcurrentTask> subTasks = new ArrayList<>();
      for (String link : result.getLinks()) {
        subTasks.add(new CustomConcurrentTask(link, deadline, maxDepth - 1, counts, visitedUrls));
      }
      invokeAll(subTasks);
      for (CustomConcurrentTask subTask : subTasks) {
        Map<String, Integer> subCounts = subTask.join();
        for (Map.Entry<String, Integer> e : subCounts.entrySet()) {
          localCounts.merge(e.getKey(), e.getValue(), Integer::sum);
        }
      }
      return localCounts;
    }
  }

}
