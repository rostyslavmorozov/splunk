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

import com.google.common.base.Strings;
import com.splunk.HttpService;
import com.splunk.Job;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import com.splunk.ServiceInfo;
import io.cdap.plugin.splunk.common.util.SearchHelper;
import io.cdap.plugin.splunk.source.batch.SplunkBatchSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Splunk Client Wrapper.
 */
public class SplunkSearchClient {

  private final SplunkBatchSourceConfig config;
  private final Service splunkService;

  public SplunkSearchClient(SplunkBatchSourceConfig config) {
    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
    this.config = config;
    this.splunkService = Service.connect(config.getConnectionArguments());
  }

  /**
   * Checks connection to the service by testing API endpoint, in case
   * of exception would be generated {@link RuntimeException}
   */
  public void checkConnection() {
    ServiceInfo info = splunkService.getInfo();
    info.getVersion();
  }

  /**
   * Prepares Splunk search.
   *
   * @return Search Id
   */
  public String prepareSearch() {
    if (SplunkBatchSourceConfig.ONESHOT_JOB.equals(config.getExecutionMode())) {
      return "";
    }
    return getSearchId(config, Long.MAX_VALUE);
  }

  /**
   * Builds {@link SplunkSearchIterator} for Splunk search.
   *
   * @param searchId Splunk search id
   * @param offset   start index of data
   * @param count    number of records to return
   * @return {@link SplunkSearchIterator}
   */
  public SplunkSearchIterator buildSearchIterator(String searchId, Long offset, Long count) {
    return new SplunkSearchIterator(splunkService, config, searchId, offset, count);
  }

  /**
   * Returns sample data for Splunk Search.
   *
   * @return List of events
   * @throws IOException thrown if there are any issue with the I/O operations.
   */
  public List<Map<String, String>> getSample() throws IOException {
    long countOfSamples = 100L;
    SplunkBatchSourceConfig configForSchema = getConfigForSchema("Normal");
    String searchId = getSearchId(configForSchema, countOfSamples);
    List<Map<String, String>> sample = new ArrayList<>();
    try (SplunkSearchIterator iterator = buildSearchIterator(searchId, 0L, countOfSamples)) {
      iterator.forEachRemaining(sample::add);
    }
    return sample;
  }

  /**
   * Returns total results for Splunk search.
   *
   * @return Total results for search
   * @throws IOException thrown if there are any issue with the I/O operations.
   */
  public long getTotalResults() throws IOException {
    long countOfRecords = 1L;
    SplunkBatchSourceConfig configForSchema = getConfigForSchema("Blocking");
    String searchId = getSearchId(configForSchema, countOfRecords);
    try (SplunkSearchIterator iterator = buildSearchIterator(searchId, 0L, countOfRecords)) {
      iterator.hasNext();
    }
    Job job = splunkService.getJob(searchId);
    return job.getResultCount();
  }

  private SplunkBatchSourceConfig getConfigForSchema(String executionMode) {
    return new SplunkBatchSourceConfig(config.referenceName,
                                       config.getAuthenticationTypeString(),
                                       config.getToken(),
                                       config.getUsername(),
                                       config.getConnectTimeout(),
                                       config.getReadTimeout(),
                                       config.getNumberOfRetries(),
                                       config.getMaxRetryWait(),
                                       config.getMaxRetryJitterWait(),
                                       config.getUrlString(),
                                       config.getPassword(),
                                       executionMode,
                                       config.getOutputFormat(),
                                       config.getSearchString(),
                                       config.getSearchId(),
                                       config.getAutoCancel(),
                                       config.getEarliestTime(),
                                       config.getLatestTime(),
                                       config.getIndexedEarliestTime(),
                                       config.getIndexedLatestTime(),
                                       config.getSearchResultsCount(),
                                       config.getSchema());
  }

  private String getSearchId(SplunkBatchSourceConfig config, Long countOfRecords) {
    Job job = getJob(config);
    while (!job.isDone()) {
      if (!job.isReady()) {
        sleep();
        continue;
      }
      if (countOfRecords <= job.getResultCount()) {
        return job.getSid();
      }
      sleep();
    }
    return job.getSid();
  }

  private void sleep() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      // no-op
    }
  }

  private Job getJob(SplunkBatchSourceConfig config) {
    if (!Strings.isNullOrEmpty(config.getSearchString())) {
      String query = SearchHelper.decorateSearchString(config);
      return splunkService.search(query, config.getSearchArguments());
    }
    return splunkService.getJob(config.getSearchId());
  }
}
