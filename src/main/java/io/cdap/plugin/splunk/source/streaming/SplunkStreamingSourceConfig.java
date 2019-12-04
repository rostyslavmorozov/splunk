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

package io.cdap.plugin.splunk.source.streaming;

import com.splunk.Job;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;

import java.io.InputStream;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class {@link SplunkStreamingSourceConfig} provides all the configuration required for
 * configuring the {@link SplunkStreamingSource} plugin.
 */
public class SplunkStreamingSourceConfig extends SplunkSourceConfig {

  public static final String SEARCH_MODE = "search_mode";
  public static final String SEARCH_REALTIME = "realtime";

  public static final String NAME_POLL_INTERVAL = "pollInterval";

  @Name(NAME_POLL_INTERVAL)
  @Description("The amount of time to wait between each poll in milliseconds. " +
    "Defaults to 60000 (1 minute).")
  private Long pollInterval;

  public SplunkStreamingSourceConfig(String referenceName,
                                     String url,
                                     String authenticationType,
                                     @Nullable String token,
                                     @Nullable String username,
                                     Integer connectTimeout,
                                     Integer readTimeout,
                                     Integer numberOfRetries,
                                     Integer maxRetryWait,
                                     Integer maxRetryJitterWait,
                                     @Nullable String password,
                                     String executionMode,
                                     String outputFormat,
                                     @Nullable String searchString,
                                     @Nullable String searchId,
                                     @Nullable Long autoCancel,
                                     @Nullable String earliestTime,
                                     @Nullable String latestTime,
                                     @Nullable String indexedEarliestTime,
                                     @Nullable String indexedLatestTime,
                                     Long searchResultsCount,
                                     @Nullable String schema,
                                     Long pollInterval) {
    super(referenceName, url, authenticationType, token, username,
          connectTimeout, readTimeout, numberOfRetries, maxRetryWait,
          maxRetryJitterWait, password, executionMode, outputFormat,
          searchString, searchId, autoCancel, earliestTime, latestTime,
          indexedEarliestTime, indexedLatestTime, searchResultsCount, schema);
    this.pollInterval = pollInterval;
  }

  public Long getPollInterval() {
    return pollInterval;
  }

  public void setPollInterval(Long pollInterval) {
    this.pollInterval = pollInterval;
  }

  @Override
  public Map<String, Object> getSearchArguments() {
    Map<String, Object> searchArguments = super.getSearchArguments();
    searchArguments.put(SEARCH_MODE, SEARCH_REALTIME);
    return searchArguments;
  }

  @Override
  public InputStream getResults(Map<String, Object> resultsArguments, Job job) {
    return job.getEvents(resultsArguments);
  }
}
