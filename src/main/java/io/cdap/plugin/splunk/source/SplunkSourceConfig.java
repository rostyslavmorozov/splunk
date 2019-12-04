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

package io.cdap.plugin.splunk.source;

import com.google.common.base.Strings;
import com.splunk.HttpException;
import com.splunk.Job;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.splunk.common.AuthenticationType;
import io.cdap.plugin.splunk.common.client.SplunkSearchClient;
import io.cdap.plugin.splunk.common.config.BaseSplunkConfig;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class {@link SplunkSourceConfig} provides all the configuration required for
 * configuring the Source plugin.
 */
public class SplunkSourceConfig extends BaseSplunkConfig {

  public static final Long PARTITION_MAX_SIZE = 20000L;
  public static final String ONESHOT_JOB = "Oneshot";

  public static final String PROPERTY_PASSWORD = "password";
  public static final String PROPERTY_EXECUTION_MODE = "executionMode";
  public static final String PROPERTY_OUTPUT_FORMAT = "outputFormat";
  public static final String PROPERTY_SEARCH_STRING = "searchString";
  public static final String PROPERTY_SEARCH_ID = "searchId";
  public static final String PROPERTY_AUTO_CANCEL = "autoCancel";
  public static final String PROPERTY_EARLIEST_TIME = "earliestTime";
  public static final String PROPERTY_LATEST_TIME = "latestTime";
  public static final String PROPERTY_INDEXED_EARLIEST_TIME = "indexedEarliestTime";
  public static final String PROPERTY_INDEXED_LATEST_TIME = "indexedLatestTime";
  public static final String PROPERTY_SEARCH_RESULTS_COUNT = "searchResultsCount";

  @Name(PROPERTY_PASSWORD)
  @Description("Password for authentication to the Splunk API.")
  @Macro
  @Nullable
  private String password;

  @Name(PROPERTY_EXECUTION_MODE)
  @Description("Defines the behaviour for the Splunk Search. " +
    "Valid values: (Blocking | Oneshot | Normal). Default is Normal.")
  @Macro
  private String executionMode;

  @Name(PROPERTY_OUTPUT_FORMAT)
  @Description("Specifies the format for the returned output. " +
    "Valid values: (csv | json | xml). Default is xml.")
  @Macro
  private String outputFormat;

  @Name(PROPERTY_SEARCH_STRING)
  @Description("Splunk Search String for retrieving results.")
  @Macro
  @Nullable
  private String searchString;

  @Name(PROPERTY_SEARCH_ID)
  @Description("Search Id for retrieving job results.")
  @Macro
  @Nullable
  private String searchId;

  @Name(PROPERTY_AUTO_CANCEL)
  @Description("The job automatically cancels after this many seconds of inactivity. " +
    "0 means never auto-cancel. Default is 0.")
  @Macro
  @Nullable
  private Long autoCancel;

  @Name(PROPERTY_EARLIEST_TIME)
  @Description("A time string. " +
    "Sets the earliest (inclusive), respectively, time bounds for the search. " +
    "The time string can be either a UTC time (with fractional seconds), " +
    "a relative time specifier (to now) or a formatted time string.")
  @Macro
  @Nullable
  private String earliestTime;

  @Name(PROPERTY_LATEST_TIME)
  @Description("A time string. " +
    "Sets the latest (exclusive), respectively, time bounds for the search. " +
    "The time string can be either a UTC time (with fractional seconds), " +
    "a relative time specifier (to now) or a formatted time string.")
  @Macro
  @Nullable
  private String latestTime;

  @Name(PROPERTY_INDEXED_EARLIEST_TIME)
  @Description("A time string. " +
    "Sets the earliest (inclusive), respectively, time bounds for the search, based on the index time bounds. " +
    "The time string can be either a UTC time (with fractional seconds), " +
    "a relative time specifier (to now) or a formatted time string.")
  @Macro
  @Nullable
  private String indexedEarliestTime;

  @Name(PROPERTY_INDEXED_LATEST_TIME)
  @Description("A time string. " +
    "Sets the latest (exclusive), respectively, time bounds for the search, based on the index time bounds. " +
    "The time string can be either a UTC time (with fractional seconds), " +
    "a relative time specifier (to now) or a formatted time string.")
  @Macro
  @Nullable
  private String indexedLatestTime;

  @Name(PROPERTY_SEARCH_RESULTS_COUNT)
  @Description("The maximum number of results to return. " +
    "If value is set to 0, then all available results are returned. Default is 0.")
  @Macro
  private Long searchResultsCount;

  @Name(PROPERTY_SCHEMA)
  @Nullable
  @Description("Output schema for the source.")
  private String schema;

  public SplunkSourceConfig(String referenceName,
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
                            @Nullable String schema) {
    super(referenceName, url, authenticationType, token, username,
          connectTimeout, readTimeout, numberOfRetries,
          maxRetryWait, maxRetryJitterWait);
    this.password = password;
    this.executionMode = executionMode;
    this.outputFormat = outputFormat;
    this.searchString = searchString;
    this.searchId = searchId;
    this.autoCancel = autoCancel;
    this.earliestTime = earliestTime;
    this.latestTime = latestTime;
    this.indexedEarliestTime = indexedEarliestTime;
    this.indexedLatestTime = indexedLatestTime;
    this.searchResultsCount = searchResultsCount;
    this.schema = schema;
  }

  public String getExecutionMode() {
    return executionMode;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  @Nullable
  public String getSearchString() {
    return searchString;
  }

  @Nullable
  public String getSearchId() {
    return searchId;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public Long getAutoCancel() {
    return autoCancel;
  }

  @Nullable
  public String getEarliestTime() {
    return earliestTime;
  }

  @Nullable
  public String getLatestTime() {
    return latestTime;
  }

  @Nullable
  public String getIndexedEarliestTime() {
    return indexedEarliestTime;
  }

  @Nullable
  public String getIndexedLatestTime() {
    return indexedLatestTime;
  }

  public Long getSearchResultsCount() {
    return searchResultsCount;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  /**
   * Returns connection properties required for Splunk client.
   *
   * @return map of arguments
   */
  @Override
  public Map<String, Object> getConnectionArguments() {
    Map<String, Object> arguments = super.getConnectionArguments();
    if (!Strings.isNullOrEmpty(getToken())) {
      arguments.put("token", "Bearer " + getToken());
    }
    if (!Strings.isNullOrEmpty(getUsername())) {
      arguments.put("username", getUsername());
    }
    if (!Strings.isNullOrEmpty(password)) {
      arguments.put("password", password);
    }
    return arguments;
  }

  /**
   * Returns query properties required for Splunk search.
   *
   * @return map of arguments
   */
  public Map<String, Object> getSearchArguments() {
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("exec_mode", executionMode.toLowerCase());
    if (autoCancel != null) {
      arguments.put("auto_cancel", autoCancel);
    }
    if (!Strings.isNullOrEmpty(earliestTime)) {
      arguments.put("earliest_time", earliestTime);
    }
    if (!Strings.isNullOrEmpty(latestTime)) {
      arguments.put("latest_time", latestTime);
    }
    if (!Strings.isNullOrEmpty(indexedEarliestTime)) {
      arguments.put("index_earliest", indexedEarliestTime);
    }
    if (!Strings.isNullOrEmpty(indexedLatestTime)) {
      arguments.put("index_latest", indexedLatestTime);
    }
    return arguments;
  }

  /**
   * Returns query properties required for Splunk results.
   *
   * @param offset start index of data
   * @param count  number of records to return
   * @return map of arguments
   */
  public Map<String, Object> getResultsArguments(Long offset, Long count) {
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("output_mode", outputFormat);
    arguments.put("offset", offset);
    arguments.put("count", count);
    return arguments;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(PROPERTY_SEARCH_STRING)
      && !Strings.isNullOrEmpty(searchString)) {
      String search = searchString.trim();
      if (!search.startsWith("search")
        && !search.startsWith("|")) {
        collector.addFailure(
          String.format("Search String '%s' has to start with the 'search' operator " +
                          "or another generating command.", searchString), null)
          .withConfigProperty(PROPERTY_SEARCH_STRING);
      }
    }
    if (!containsMacro(PROPERTY_SEARCH_STRING)
      && !containsMacro(PROPERTY_SEARCH_ID)) {
      if (Strings.isNullOrEmpty(searchString)
        && Strings.isNullOrEmpty(searchId)) {
        collector.addFailure("Search String or Search Id must be specified.", null)
          .withConfigProperty(PROPERTY_SEARCH_STRING)
          .withConfigProperty(PROPERTY_SEARCH_ID);
      }
    }
    if (!containsMacro(PROPERTY_EXECUTION_MODE)
      && !containsMacro(PROPERTY_SEARCH_ID)) {
      if (ONESHOT_JOB.equals(executionMode)
        && !Strings.isNullOrEmpty(searchId)) {
        collector.addFailure("Search Id is not supported in Execution Mode 'Oneshot'.",
                             null)
          .withConfigProperty(PROPERTY_EXECUTION_MODE)
          .withConfigProperty(PROPERTY_SEARCH_ID);
      }
    }
  }

  @Override
  public void validateConnection(FailureCollector collector) {
    boolean hasAuthErrors = checkAuthProperties(collector);
    if (containsMacro(PROPERTY_URL) || hasAuthErrors) {
      return;
    }
    AuthenticationType authenticationType = getAuthenticationType();
    try {
      SplunkSearchClient client = new SplunkSearchClient(this);
      client.checkConnection();
    } catch (HttpException e) {
      switch (authenticationType) {
        case BASIC:
          collector.addFailure(
            String.format("There was an issue communicating with Splunk API: '%s'.",
                          e.getDetail()), null)
            .withConfigProperty(PROPERTY_USERNAME)
            .withConfigProperty(PROPERTY_PASSWORD);
          break;
        case TOKEN:
          collector.addFailure(String.format("There was an issue communicating with Splunk API: '%s'.",
                                             e.getDetail()), null)
            .withStacktrace(e.getStackTrace())
            .withConfigProperty(PROPERTY_TOKEN);
          break;
      }
    } catch (RuntimeException e) {
      /*
       * In case wrong port specified Splunk can throw:
       * java.lang.RuntimeException: Unrecognized SSL message, plaintext connection?
       *
       * In case wrong protocol specified Splunk can throw:
       * java.lang.RuntimeException: Unexpected end of file from server
       */
      switch (authenticationType) {
        case BASIC:
          collector.addFailure("There was an issue communicating with Splunk API.", null)
            .withStacktrace(e.getStackTrace())
            .withConfigProperty(PROPERTY_USERNAME)
            .withConfigProperty(PROPERTY_PASSWORD);
          break;
        case TOKEN:
          collector.addFailure("There was an issue communicating with Splunk API.", null)
            .withStacktrace(e.getStackTrace())
            .withConfigProperty(PROPERTY_TOKEN);
          break;
      }
    }
  }

  private boolean checkAuthProperties(FailureCollector collector) {
    boolean hasPropertyErrors = false;

    AuthenticationType authenticationType = getAuthenticationType();
    switch (authenticationType) {
      case BASIC:
        if (containsMacro(PROPERTY_USERNAME)
          || containsMacro(PROPERTY_PASSWORD)) {
          break;
        }
        if (Strings.isNullOrEmpty(getUsername())) {
          collector.addFailure("User name is not set.", null)
            .withConfigProperty(PROPERTY_USERNAME);
          hasPropertyErrors = true;
        }
        if (Strings.isNullOrEmpty(password)) {
          collector.addFailure("Password is not set.", null)
            .withConfigProperty(PROPERTY_PASSWORD);
          hasPropertyErrors = true;
        }
        break;
      case TOKEN:
        if (containsMacro(PROPERTY_TOKEN)) {
          break;
        }
        if (Strings.isNullOrEmpty(getToken())) {
          collector.addFailure("API Token is not set.", null)
            .withConfigProperty(PROPERTY_TOKEN);
          hasPropertyErrors = true;
        }
        break;
    }
    return hasPropertyErrors;
  }

  public InputStream getResults(Map<String, Object> resultsArguments, Job job) {
    return job.getResults(resultsArguments);
  }
}
