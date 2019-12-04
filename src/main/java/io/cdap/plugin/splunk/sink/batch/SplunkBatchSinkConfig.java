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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.splunk.HttpException;
import com.splunk.RequestMessage;
import com.splunk.Service;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.splunk.common.AuthenticationType;
import io.cdap.plugin.splunk.common.config.BaseSplunkConfig;
import io.cdap.plugin.splunk.common.util.SplunkCollectorHelper;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class {@link SplunkBatchSinkConfig} provides all the configuration required for
 * configuring the {@link SplunkBatchSink} plugin.
 */
public class SplunkBatchSinkConfig extends BaseSplunkConfig {

  private static final Gson GSON = new GsonBuilder().create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  public static final String ENPOINT_EVENT = "Event";
  public static final String ENPOINT_RAW = "Raw";
  public static final String ENDPOINT_HEALTH = "health";

  public static final String PROPERTY_ENDPOINT = "endpoint";
  public static final String PROPERTY_BATCH_SIZE = "batchSize";
  public static final String PROPERTY_EVENT_METADATA = "eventMetadata";
  public static final String PROPERTY_CHANNEL_IDENTIFIER_HEADER = "channelIdentifierHeader";

  @Name(PROPERTY_ENDPOINT)
  @Description("Splunk endpoint to send data to.")
  private String endpoint;

  @Name(PROPERTY_BATCH_SIZE)
  @Description("The number of messages to batch before sending. Default is 1.")
  @Macro
  private Integer batchSize;

  @Name(PROPERTY_EVENT_METADATA)
  @Description("Optional event metadata string in the JSON export for destination.")
  @Nullable
  private String eventMetadata;

  @Name(PROPERTY_CHANNEL_IDENTIFIER_HEADER)
  @Description("GUID for Splunk Channel.")
  @Macro
  @Nullable
  private String channelIdentifierHeader;

  public SplunkBatchSinkConfig(String referenceName,
                               String url,
                               String authenticationType,
                               @Nullable String token,
                               @Nullable String username,
                               Integer connectTimeout,
                               Integer readTimeout,
                               Integer numberOfRetries,
                               Integer maxRetryWait,
                               Integer maxRetryJitterWait,
                               String endpoint,
                               Integer batchSize,
                               @Nullable String eventMetadata,
                               @Nullable String channelIdentifierHeader) {
    super(referenceName, url, authenticationType, token,
          username, connectTimeout, readTimeout,
          numberOfRetries, maxRetryWait, maxRetryJitterWait);
    this.endpoint = endpoint;
    this.batchSize = batchSize;
    this.eventMetadata = eventMetadata;
    this.channelIdentifierHeader = channelIdentifierHeader;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  @Nullable
  public String getEventMetadata() {
    return eventMetadata;
  }

  @Nullable
  public String getChannelIdentifierHeader() {
    return channelIdentifierHeader;
  }

  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);
    if (schema == null || schema.getFields() == null || schema.getFields().isEmpty()) {
      collector.addFailure("Sink schema must contain at least one field.", null)
        .withConfigProperty(PROPERTY_SCHEMA);
    }
    try {
      getEventMetadataMap();
    } catch (IllegalArgumentException e) {
      collector.addFailure("'Event Metadata' property must be a valid json.", null)
        .withConfigProperty(PROPERTY_EVENT_METADATA);
    }
    if (!containsMacro(PROPERTY_CHANNEL_IDENTIFIER_HEADER)) {
      if (ENPOINT_RAW.equals(endpoint) && Strings.isNullOrEmpty(channelIdentifierHeader)) {
        collector.addFailure(
          String.format("'Channel Identifier Header' property can't be empty for '%s' endpoint.",
                        ENPOINT_RAW), null)
          .withConfigProperty(PROPERTY_CHANNEL_IDENTIFIER_HEADER);
      }
    }
  }

  @Override
  public void validateConnection(FailureCollector collector) {
    boolean hasAuthErrors = hasAuthErrors(collector);
    if (containsMacro(PROPERTY_URL) || hasAuthErrors) {
      return;
    }

    AuthenticationType authenticationType = getAuthenticationType();
    try {
      Service splunkService = Service.connect(getConnectionArguments());
      String basicAuth = SplunkCollectorHelper.buildBasicAuth(this);
      String path = SplunkCollectorHelper.buildPath(this, ENDPOINT_HEALTH);
      RequestMessage request = SplunkCollectorHelper
        .buildRequest(this, basicAuth, "GET", null);
      SplunkCollectorHelper.sendRequest(splunkService, path, request);
    } catch (HttpException e) {
      switch (authenticationType) {
        case BASIC:
          collector.addFailure(
            String.format("There was an issue communicating with Splunk API: '%s'.",
                          e.getDetail()), null)
            .withConfigProperty(PROPERTY_USERNAME)
            .withConfigProperty(PROPERTY_TOKEN);
          break;
        case TOKEN:
        case QUERY_STRING:
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
            .withConfigProperty(PROPERTY_TOKEN);
          break;
        case TOKEN:
        case QUERY_STRING:
          collector.addFailure("There was an issue communicating with Splunk API.", null)
            .withStacktrace(e.getStackTrace())
            .withConfigProperty(PROPERTY_TOKEN);
          break;
      }
    }
  }

  private boolean hasAuthErrors(FailureCollector collector) {
    AuthenticationType authenticationType = getAuthenticationType();
    switch (authenticationType) {
      case QUERY_STRING:
      case TOKEN:
        if (!containsMacro(PROPERTY_TOKEN)
          && Strings.isNullOrEmpty(getToken())) {
          collector.addFailure("API Token is not set.", null)
            .withConfigProperty(PROPERTY_TOKEN);
          return true;
        }
        break;
      case BASIC:
        if (!containsMacro(PROPERTY_USERNAME)
          && !containsMacro(PROPERTY_TOKEN)) {
          boolean hasPropertyErrors = false;
          if (Strings.isNullOrEmpty(getUsername())) {
            collector.addFailure("User name is not set.", null)
              .withConfigProperty(PROPERTY_USERNAME);
            hasPropertyErrors = true;
          }
          if (Strings.isNullOrEmpty(getToken())) {
            collector.addFailure("API Token is not set.", null)
              .withConfigProperty(PROPERTY_TOKEN);
            hasPropertyErrors = true;
          }
          return hasPropertyErrors;
        }
        break;
    }
    return false;
  }

  /**
   * Returns event metadata map.
   *
   * @return event metadata map
   */
  public Map<String, String> getEventMetadataMap() {
    if (Strings.isNullOrEmpty(eventMetadata)) {
      return Collections.emptyMap();
    }
    try {
      return GSON.fromJson(eventMetadata, MAP_TYPE);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Unable to parse eventMetadata property: " + e.getMessage(), e);
    }
  }

  /**
   * Returns connection properties required for Splunk client.
   *
   * @return map of arguments
   */
  @Override
  public Map<String, Object> getConnectionArguments() {
    Map<String, Object> arguments = super.getConnectionArguments();
    if (getAuthenticationType() == AuthenticationType.TOKEN
      && !Strings.isNullOrEmpty(getToken())) {
      arguments.put("token", "Splunk " + getToken());
    }
    return arguments;
  }
}
