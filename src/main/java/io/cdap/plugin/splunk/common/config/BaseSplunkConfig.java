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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.splunk.common.AuthenticationType;

import javax.annotation.Nullable;

/**
 * Base configuration for Splunk plugins.
 */
public class BaseSplunkConfig extends ReferencePluginConfig {

  public static final String PROPERTY_AUTHENTICATION_TYPE = "authenticationType";
  public static final String PROPERTY_TOKEN = "token";
  public static final String PROPERTY_USERNAME = "username";
  public static final String PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  public static final String PROPERTY_READ_TIMEOUT = "readTimeout";
  public static final String PROPERTY_NUMBER_OF_RETRIES = "numberOfRetries";
  public static final String PROPERTY_MAX_RETRY_WAIT = "maxRetryWait";
  public static final String PROPERTY_MAX_RETRY_JITTER_WAIT = "maxRetryJitterWait";

  @Name(PROPERTY_AUTHENTICATION_TYPE)
  @Description("Authentication method to access Splunk API. " +
    "Defaults to Basic Authentication.")
  private String authenticationType;

  @Name(PROPERTY_TOKEN)
  @Description("The value of token created for authentication to the Splunk API.")
  @Macro
  @Nullable
  private String token;

  @Name(PROPERTY_USERNAME)
  @Description("Login name for authentication to the Splunk API.")
  @Macro
  @Nullable
  private String username;

  @Name(PROPERTY_CONNECT_TIMEOUT)
  @Description("The time in milliseconds to wait for a connection. Set to 0 for infinite. " +
    "Defaults to 60000 (1 minute).")
  @Macro
  private Integer connectTimeout;

  @Name(PROPERTY_READ_TIMEOUT)
  @Description("The time in milliseconds to wait for a read. Set to 0 for infinite. " +
    "Defaults to 60000 (1 minute).")
  @Macro
  private Integer readTimeout;

  @Name(PROPERTY_NUMBER_OF_RETRIES)
  @Description("The number of times the request should be retried if the request fails. " +
    "Defaults to 3.")
  @Macro
  private Integer numberOfRetries;

  @Name(PROPERTY_MAX_RETRY_WAIT)
  @Description("Maximum time in seconds retries can take. Set to 0 for infinite. " +
    "Defaults to 60000 (1 minute).")
  @Macro
  private Integer maxRetryWait;

  @Name(PROPERTY_MAX_RETRY_JITTER_WAIT)
  @Description("Maximum time in milliseconds added to retries. Defaults to 100.")
  @Macro
  private Integer maxRetryJitterWait;

  public BaseSplunkConfig(String referenceName,
                          String authenticationType,
                          @Nullable String token,
                          @Nullable String username,
                          Integer connectTimeout,
                          Integer readTimeout,
                          Integer numberOfRetries,
                          Integer maxRetryWait,
                          Integer maxRetryJitterWait) {
    super(referenceName);
    this.authenticationType = authenticationType;
    this.token = token;
    this.username = username;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.numberOfRetries = numberOfRetries;
    this.maxRetryWait = maxRetryWait;
    this.maxRetryJitterWait = maxRetryJitterWait;
  }

  public String getAuthenticationTypeString() {
    return authenticationType;
  }

  public AuthenticationType getAuthenticationType() {
    switch (authenticationType) {
      case "basic":
        return AuthenticationType.BASIC;
      case "token":
        return AuthenticationType.TOKEN;
      default:
        throw new IllegalArgumentException(
          String.format("Authentication using '%s' is not supported.", authenticationType));
    }
  }

  public String getToken() {
    return token;
  }

  public String getUsername() {
    return username;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public Integer getReadTimeout() {
    return readTimeout;
  }

  public Integer getNumberOfRetries() {
    return numberOfRetries;
  }

  public Integer getMaxRetryWait() {
    return maxRetryWait;
  }

  public Integer getMaxRetryJitterWait() {
    return maxRetryJitterWait;
  }

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
    try {
      getAuthenticationType();
    } catch (IllegalArgumentException e) {
      collector.addFailure(String.format("Invalid authentication type: %s.",
                                         authenticationType), null)
        .withConfigProperty(PROPERTY_AUTHENTICATION_TYPE);
    }
  }
}
