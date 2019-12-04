/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.splunk.common.client;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.annotations.VisibleForTesting;
import com.splunk.Event;
import com.splunk.Job;
import com.splunk.ResultsReader;
import com.splunk.ResultsReaderCsv;
import com.splunk.ResultsReaderJson;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import io.cdap.plugin.splunk.common.exception.ConnectionTimeoutException;
import io.cdap.plugin.splunk.common.util.SearchHelper;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

/**
 * Iterator for Splunk Search.
 */
public class SplunkSearchIterator implements Iterator<Map<String, String>>, Closeable {

  private static final Pattern RESTRICTED_PATTERN = Pattern.compile("[^a-zA-Z0-9_]");

  private final Service splunkService;
  private final SplunkSourceConfig config;
  private final String searchId;
  private final Long offset;
  private final Long count;
  private long iteratorPosition = 0L;

  private ResultsReader resultsReader;
  private Iterator<Event> iterator;

  public SplunkSearchIterator(Service splunkService, SplunkSourceConfig config,
                              String searchId, Long offset, Long count) {
    this.splunkService = splunkService;
    this.config = config;
    this.searchId = searchId;
    this.offset = offset;
    this.count = count;
  }

  public SplunkSearchIterator(SplunkSourceConfig config,
                              String searchId, Long offset, Long count) {
    this.splunkService = SearchHelper.buildSplunkService(config);
    this.config = config;
    this.searchId = searchId;
    this.offset = offset;
    this.count = count;
  }

  @Override
  public boolean hasNext() {
    if (resultsReader == null || iterator == null) {
      try {
        InputStream stream = getStreamResults(config, searchId, offset, count);
        resultsReader = getResultsReader(stream, config.getOutputFormat());
        iterator = resultsReader.iterator();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return iterator.hasNext();
  }

  @Override
  public Map<String, String> next() {
    iteratorPosition++;
    Map<String, String> event = iterator.next();
    return cleanUpFieldNames(event);
  }

  @Override
  public void close() throws IOException {
    if (resultsReader != null) {
      resultsReader.close();
    }
  }

  public long getIteratorPosition() {
    return iteratorPosition;
  }

  public void setIteratorPosition(long iteratorPosition) {
    this.iteratorPosition = iteratorPosition;
  }

  @VisibleForTesting
  ResultsReader getResultsReader(InputStream stream, String outputFormat) throws IOException {
    switch (outputFormat) {
      case "csv":
        return new ResultsReaderCsv(stream);
      case "json":
        return new ResultsReaderJson(stream);
      case "xml":
        return new ResultsReaderXml(stream);
    }
    throw new IllegalArgumentException(String.format("Unsupported output format '%s'", outputFormat));
  }

  @VisibleForTesting
  InputStream getStreamResults(SplunkSourceConfig config, String searchId,
                               long offset, Long count) {
    Map<String, Object> resultsArguments = config.getResultsArguments(offset, count);
    if (SplunkSourceConfig.ONESHOT_JOB.equals(config.getExecutionMode())) {
      String query = SearchHelper.decorateSearchString(config);
      resultsArguments.putAll(config.getSearchArguments());
      return splunkService.oneshotSearch(query, resultsArguments);
    }

    Job job = splunkService.getJob(searchId);
    Retryer<InputStream> retryer = SearchHelper.buildRetryer(config);
    try {
      return retryer.call(() -> getInputStream(config, resultsArguments, job));
    } catch (ExecutionException | RetryException e) {
      throw new ConnectionTimeoutException(
        String.format("Cannot create Splunk connection for search: '%s'", searchId), e);
    }
  }

  private InputStream getInputStream(SplunkSourceConfig config,
                                     Map<String, Object> resultsArguments,
                                     Job job) {
    return SearchHelper.wrapRetryCall(() -> config.getResults(resultsArguments, job));
  }

  private Map<String, String> cleanUpFieldNames(Map<String, String> event) {
    Map<String, String> result = new HashMap<>();
    event.keySet().forEach(key -> {
      String formatted = RESTRICTED_PATTERN.matcher(key).replaceAll("");
      result.put(formatted, event.get(key));
    });
    return result;
  }
}
