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

package io.cdap.plugin.splunk.sink.batch;

/**
 * Helper class to simplify {@link SplunkBatchSinkConfig} class creation.
 */
public class SplunkBatchSinkConfigBuilder {

  public static final SplunkBatchSinkConfig CONFIG = new SplunkBatchSinkConfig(
    "reference",
    "http://localhost:8088",
    "basic",
    "apiToken",
    "userName",
    60000,
    60000,
    3,
    60000,
    100,
    SplunkBatchSinkConfig.ENPOINT_EVENT,
    1,
    "",
    "");

  private String referenceName;
  private String url;
  private String authenticationType;
  private String token;
  private String username;
  private Integer connectTimeout;
  private Integer readTimeout;
  private Integer numberOfRetries;
  private Integer maxRetryWait;
  private Integer maxRetryJitterWait;
  private String endpoint;
  private Integer batchSize;
  private String eventMetadata;
  private String channelIdentifierHeader;

  public SplunkBatchSinkConfigBuilder() {
  }

  public SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfig config) {
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
    this.endpoint = config.getEndpoint();
    this.batchSize = config.getBatchSize();
    this.eventMetadata = config.getEventMetadata();
    this.channelIdentifierHeader = config.getChannelIdentifierHeader();
  }

  public SplunkBatchSinkConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setUrl(String url) {
    this.url = url;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setAuthenticationType(String authenticationType) {
    this.authenticationType = authenticationType;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setToken(String token) {
    this.token = token;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setConnectTimeout(Integer connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setReadTimeout(Integer readTimeout) {
    this.readTimeout = readTimeout;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setNumberOfRetries(Integer numberOfRetries) {
    this.numberOfRetries = numberOfRetries;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setMaxRetryWait(Integer maxRetryWait) {
    this.maxRetryWait = maxRetryWait;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setMaxRetryJitterWait(Integer maxRetryJitterWait) {
    this.maxRetryJitterWait = maxRetryJitterWait;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setEndpoint(String endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setEventMetadata(String eventMetadata) {
    this.eventMetadata = eventMetadata;
    return this;
  }

  public SplunkBatchSinkConfigBuilder setChannelIdentifierHeader(String channelIdentifierHeader) {
    this.channelIdentifierHeader = channelIdentifierHeader;
    return this;
  }

  public SplunkBatchSinkConfig build() {
    return new SplunkBatchSinkConfig(referenceName,
                                     url,
                                     authenticationType,
                                     token,
                                     username,
                                     connectTimeout,
                                     readTimeout,
                                     numberOfRetries,
                                     maxRetryWait,
                                     maxRetryJitterWait,
                                     endpoint,
                                     batchSize,
                                     eventMetadata,
                                     channelIdentifierHeader);
  }
}
