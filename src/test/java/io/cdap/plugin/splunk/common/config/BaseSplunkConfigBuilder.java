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

package io.cdap.plugin.splunk.common.config;

/**
 * Helper class to simplify {@link BaseSplunkConfig} class creation.
 */
public class BaseSplunkConfigBuilder {

  public static final BaseSplunkConfig CONFIG = new BaseSplunkConfig(
    "reference",
    "https://localhost:8089",
    "basic",
    "apiToken",
    "userName",
    60000,
    60000,
    3,
    60000,
    100);

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

  public BaseSplunkConfigBuilder() {
  }

  public BaseSplunkConfigBuilder(BaseSplunkConfig config) {
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
  }

  public BaseSplunkConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public BaseSplunkConfigBuilder setUrl(String url) {
    this.url = url;
    return this;
  }

  public BaseSplunkConfigBuilder setAuthenticationType(String authenticationType) {
    this.authenticationType = authenticationType;
    return this;
  }

  public BaseSplunkConfigBuilder setToken(String token) {
    this.token = token;
    return this;
  }

  public BaseSplunkConfigBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  public BaseSplunkConfigBuilder setConnectTimeout(Integer connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public BaseSplunkConfigBuilder setReadTimeout(Integer readTimeout) {
    this.readTimeout = readTimeout;
    return this;
  }

  public BaseSplunkConfigBuilder setNumberOfRetries(Integer numberOfRetries) {
    this.numberOfRetries = numberOfRetries;
    return this;
  }

  public BaseSplunkConfigBuilder setMaxRetryWait(Integer maxRetryWait) {
    this.maxRetryWait = maxRetryWait;
    return this;
  }

  public BaseSplunkConfigBuilder setMaxRetryJitterWait(Integer maxRetryJitterWait) {
    this.maxRetryJitterWait = maxRetryJitterWait;
    return this;
  }

  public BaseSplunkConfig build() {
    return new BaseSplunkConfig(referenceName,
                                url,
                                authenticationType,
                                token,
                                username,
                                connectTimeout,
                                readTimeout,
                                numberOfRetries,
                                maxRetryWait,
                                maxRetryJitterWait);
  }
}
