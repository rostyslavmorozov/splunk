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

package io.cdap.plugin.splunk.common.util;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Strings;
import io.cdap.plugin.splunk.common.exception.JobResultsException;
import io.cdap.plugin.splunk.source.batch.SplunkBatchSourceConfig;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * Search helper methods.
 */
public class SearchHelper {

  public static String decorateSearchString(SplunkBatchSourceConfig config) {
    String searchString = config.getSearchString();
    if (Strings.isNullOrEmpty(searchString)) {
      return searchString;
    }
    String search = searchString.trim();
    return search.contains("kvform") ? search : search + " | kvform";
  }

  public static Retryer<InputStream> buildRetryer(SplunkBatchSourceConfig config) {
    return RetryerBuilder.<InputStream>newBuilder()
      .retryIfExceptionOfType(JobResultsException.class)
      .withWaitStrategy(WaitStrategies.join(
        WaitStrategies.exponentialWait(config.getMaxRetryWait(), TimeUnit.MILLISECONDS),
        WaitStrategies.randomWait(config.getMaxRetryJitterWait(), TimeUnit.MILLISECONDS)))
      .withStopStrategy(StopStrategies.stopAfterAttempt(config.getNumberOfRetries()))
      .build();
  }
}
