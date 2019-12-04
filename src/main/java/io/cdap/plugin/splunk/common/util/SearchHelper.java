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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Strings;
import com.splunk.Service;
import io.cdap.plugin.splunk.common.config.BaseSplunkConfig;
import io.cdap.plugin.splunk.common.exception.ConnectionTimeoutException;
import io.cdap.plugin.splunk.common.exception.JobResultsException;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Search helper methods.
 */
public class SearchHelper {

  private SearchHelper() {
  }

  public static String decorateSearchString(SplunkSourceConfig config) {
    String searchString = config.getSearchString();
    if (Strings.isNullOrEmpty(searchString)) {
      return searchString;
    }
    String search = searchString.trim();
    return search.contains("kvform") ? search : search + " | kvform";
  }

  public static <T> Retryer<T> buildRetryer(BaseSplunkConfig config) {
    return RetryerBuilder.<T>newBuilder()
      .retryIfExceptionOfType(JobResultsException.class)
      .withWaitStrategy(WaitStrategies.join(
        WaitStrategies.exponentialWait(config.getMaxRetryWait(), TimeUnit.MILLISECONDS),
        WaitStrategies.randomWait(config.getMaxRetryJitterWait(), TimeUnit.MILLISECONDS)))
      .withStopStrategy(StopStrategies.stopAfterAttempt(config.getNumberOfRetries()))
      .build();
  }

  public static <T> T wrapRetryCall(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      throw new JobResultsException(e);
    }
  }

  public static Service buildSplunkService(SplunkSourceConfig config) {
    Retryer<Service> retryer = SearchHelper.buildRetryer(config);
    try {
      return retryer.call(() -> getService(config));
    } catch (ExecutionException | RetryException e) {
      throw new ConnectionTimeoutException(
        String.format("Cannot create Splunk connection to: '%s'", config.getUrlString()), e);
    }
  }

  private static Service getService(SplunkSourceConfig config) {
    return wrapRetryCall(() -> Service.connect(config.getConnectionArguments()));
  }
}
