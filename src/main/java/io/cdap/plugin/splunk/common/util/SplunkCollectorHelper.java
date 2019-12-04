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

import com.google.common.base.Strings;
import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import com.splunk.Service;
import io.cdap.plugin.splunk.common.AuthenticationType;
import io.cdap.plugin.splunk.sink.batch.SplunkBatchSinkConfig;

import java.util.Base64;

/**
 * Splunk HEC collector helper methods.
 */
public class SplunkCollectorHelper {

  public static final String HEADER_X_SPLUNK_REQUEST_CHANNEL = "X-Splunk-Request-Channel";
  public static final String HEADER_AUTHORIZATION = "Authorization";

  private SplunkCollectorHelper() {
  }

  public static String buildBasicAuth(SplunkBatchSinkConfig config) {
    String userpass = config.getUsername() + ":" + config.getToken();
    return "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
  }

  public static String buildPath(SplunkBatchSinkConfig config, String endpoint) {
    return config.getAuthenticationType() == AuthenticationType.QUERY_STRING
      ? String.format("/services/collector/%s?token=%s", endpoint, config.getToken())
      : String.format("/services/collector/%s", endpoint);
  }

  public static RequestMessage buildRequest(SplunkBatchSinkConfig config,
                                            String basicAuth,
                                            String method,
                                            String content) {
    RequestMessage request = new RequestMessage(method);
    if (!Strings.isNullOrEmpty(config.getChannelIdentifierHeader())) {
      request.getHeader().put(HEADER_X_SPLUNK_REQUEST_CHANNEL, config.getChannelIdentifierHeader());
    }
    if (config.getAuthenticationType() == AuthenticationType.BASIC) {
      request.getHeader().put(HEADER_AUTHORIZATION, basicAuth);
    }
    request.setContent(content);
    return request;
  }

  public static Void sendRequest(Service splunkService, String path, RequestMessage request) {
    return SearchHelper.wrapRetryCall(() -> {
      ResponseMessage response = splunkService.send(path, request);
      response.getContent().close();
      return null;
    });
  }
}
