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

/**
 * Helper class to simplify {@link SplunkSourceConfig} class creation.
 */
public class SplunkSourceConfigBuilder {

  public static final SplunkSourceConfig CONFIG = new SplunkSourceConfig(
    "reference",
    "https://localhost:8089",
    "basic",
    "apiToken",
    "userName",
    60000,
    60000,
    3,
    60000,
    100,
    "password",
    "executionMode",
    "outputFormat",
    "searchString",
    "searchId",
    0L,
    "earliestTime",
    "latestTime",
    "indexedEarliestTime",
    "indexedLatestTime",
    100L,
    "schema");

  private String referenceName;
  private String authenticationType;
  private String token;
  private String username;
  private Integer connectTimeout;
  private Integer readTimeout;
  private Integer numberOfRetries;
  private Integer maxRetryWait;
  private Integer maxRetryJitterWait;
  private String url;
  private String password;
  private String executionMode;
  private String outputFormat;
  private String searchString;
  private String searchId;
  private Long autoCancel;
  private String earliestTime;
  private String latestTime;
  private String indexedEarliestTime;
  private String indexedLatestTime;
  private Long searchResultsCount;
  private String schema;

  public SplunkSourceConfigBuilder() {
  }

  public SplunkSourceConfigBuilder(SplunkSourceConfig config) {
    this.referenceName = config.referenceName;
    this.authenticationType = config.getAuthenticationTypeString();
    this.token = config.getToken();
    this.username = config.getUsername();
    this.connectTimeout = config.getConnectTimeout();
    this.readTimeout = config.getReadTimeout();
    this.numberOfRetries = config.getNumberOfRetries();
    this.maxRetryWait = config.getMaxRetryWait();
    this.maxRetryJitterWait = config.getMaxRetryJitterWait();
    this.url = config.getUrlString();
    this.password = config.getPassword();
    this.executionMode = config.getExecutionMode();
    this.outputFormat = config.getOutputFormat();
    this.searchString = config.getSearchString();
    this.searchId = config.getSearchId();
    this.autoCancel = config.getAutoCancel();
    this.earliestTime = config.getEarliestTime();
    this.latestTime = config.getLatestTime();
    this.indexedEarliestTime = config.getIndexedEarliestTime();
    this.indexedLatestTime = config.getIndexedLatestTime();
    this.searchResultsCount = config.getSearchResultsCount();
    this.schema = config.getSchema();
  }

  public SplunkSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SplunkSourceConfigBuilder setAuthenticationType(String authenticationType) {
    this.authenticationType = authenticationType;
    return this;
  }

  public SplunkSourceConfigBuilder setToken(String token) {
    this.token = token;
    return this;
  }

  public SplunkSourceConfigBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  public SplunkSourceConfigBuilder setConnectTimeout(Integer connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public SplunkSourceConfigBuilder setReadTimeout(Integer readTimeout) {
    this.readTimeout = readTimeout;
    return this;
  }

  public SplunkSourceConfigBuilder setNumberOfRetries(Integer numberOfRetries) {
    this.numberOfRetries = numberOfRetries;
    return this;
  }

  public SplunkSourceConfigBuilder setMaxRetryWait(Integer maxRetryWait) {
    this.maxRetryWait = maxRetryWait;
    return this;
  }

  public SplunkSourceConfigBuilder setMaxRetryJitterWait(Integer maxRetryJitterWait) {
    this.maxRetryJitterWait = maxRetryJitterWait;
    return this;
  }

  public SplunkSourceConfigBuilder setUrl(String url) {
    this.url = url;
    return this;
  }

  public SplunkSourceConfigBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public SplunkSourceConfigBuilder setExecutionMode(String executionMode) {
    this.executionMode = executionMode;
    return this;
  }

  public SplunkSourceConfigBuilder setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
    return this;
  }

  public SplunkSourceConfigBuilder setSearchString(String searchString) {
    this.searchString = searchString;
    return this;
  }

  public SplunkSourceConfigBuilder setSearchId(String searchId) {
    this.searchId = searchId;
    return this;
  }

  public SplunkSourceConfigBuilder setAutoCancel(Long autoCancel) {
    this.autoCancel = autoCancel;
    return this;
  }

  public SplunkSourceConfigBuilder setEarliestTime(String earliestTime) {
    this.earliestTime = earliestTime;
    return this;
  }

  public SplunkSourceConfigBuilder setLatestTime(String latestTime) {
    this.latestTime = latestTime;
    return this;
  }

  public SplunkSourceConfigBuilder setIndexedEarliestTime(String indexedEarliestTime) {
    this.indexedEarliestTime = indexedEarliestTime;
    return this;
  }

  public SplunkSourceConfigBuilder setIndexedLatestTime(String indexedLatestTime) {
    this.indexedLatestTime = indexedLatestTime;
    return this;
  }

  public SplunkSourceConfigBuilder setSearchResultsCount(Long searchResultsCount) {
    this.searchResultsCount = searchResultsCount;
    return this;
  }

  public SplunkSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public SplunkSourceConfig build() {
    return new SplunkSourceConfig(referenceName,
                                  url,
                                  authenticationType,
                                  token,
                                  username,
                                  connectTimeout,
                                  readTimeout,
                                  numberOfRetries,
                                  maxRetryWait,
                                  maxRetryJitterWait,
                                  password,
                                  executionMode,
                                  outputFormat,
                                  searchString,
                                  searchId,
                                  autoCancel,
                                  earliestTime,
                                  latestTime,
                                  indexedEarliestTime,
                                  indexedLatestTime,
                                  searchResultsCount,
                                  schema);
  }
}
