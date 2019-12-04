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

import com.splunk.RequestMessage;
import io.cdap.plugin.splunk.sink.batch.SplunkBatchSinkConfig;
import io.cdap.plugin.splunk.sink.batch.SplunkBatchSinkConfigBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SplunkCollectorHelper}
 */
public class SplunkCollectorHelperTest {

  @Test
  public void testBuildBasicAuth() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("basic")
        .setToken("apiToken")
        .setUsername("userName")
        .build();

    String actual = SplunkCollectorHelper.buildBasicAuth(config);
    Assert.assertEquals("Basic dXNlck5hbWU6YXBpVG9rZW4=", actual);
  }

  @Test
  public void testBuildPath() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("basic")
        .setToken("apiToken")
        .build();

    String actual = SplunkCollectorHelper.buildPath(config,
                                                    SplunkBatchSinkConfig.ENDPOINT_HEALTH);
    Assert.assertEquals("/services/collector/health", actual);
  }

  @Test
  public void testBuildPathQueryString() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("query")
        .setToken("apiToken")
        .build();

    String actual = SplunkCollectorHelper.buildPath(config,
                                                    SplunkBatchSinkConfig.ENDPOINT_HEALTH);
    Assert.assertEquals("/services/collector/health?token=apiToken", actual);
  }

  @Test
  public void testBuildRequest() {
    String expectedMethod = "method";
    String expectedContent = "content";
    String expectedBasicAuth = "basicAuth";

    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setChannelIdentifierHeader("")
        .build();

    RequestMessage actual = SplunkCollectorHelper.buildRequest(config,
                                                               expectedBasicAuth,
                                                               expectedMethod,
                                                               expectedContent);
    Assert.assertNotNull(actual);
    Assert.assertEquals(expectedMethod, actual.getMethod());
    Assert.assertNotNull(actual.getHeader());
    Assert.assertTrue(actual.getHeader().isEmpty());
    Assert.assertEquals(expectedContent, actual.getContent());
  }

  @Test
  public void testBuildRequestWithChannel() {
    String expectedChannel = "channel";
    String expectedMethod = "method";
    String expectedContent = "content";
    String expectedBasicAuth = "basicAuth";

    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setChannelIdentifierHeader(expectedChannel)
        .build();

    RequestMessage actual = SplunkCollectorHelper.buildRequest(config,
                                                               expectedBasicAuth,
                                                               expectedMethod,
                                                               expectedContent);
    Assert.assertNotNull(actual);
    Assert.assertEquals(expectedMethod, actual.getMethod());
    Assert.assertNotNull(actual.getHeader());
    Assert.assertFalse(actual.getHeader().isEmpty());
    Assert.assertEquals(1, actual.getHeader().size());
    Assert.assertEquals(expectedChannel, actual.getHeader().get(SplunkCollectorHelper.HEADER_X_SPLUNK_REQUEST_CHANNEL));
    Assert.assertEquals(expectedContent, actual.getContent());
  }

  @Test
  public void testBuildRequestWithBasicAuth() {
    String expectedMethod = "method";
    String expectedContent = "content";
    String expectedBasicAuth = "basicAuth";

    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("basic")
        .setChannelIdentifierHeader("")
        .build();

    RequestMessage actual = SplunkCollectorHelper.buildRequest(config,
                                                               expectedBasicAuth,
                                                               expectedMethod,
                                                               expectedContent);
    Assert.assertNotNull(actual);
    Assert.assertEquals(expectedMethod, actual.getMethod());
    Assert.assertNotNull(actual.getHeader());
    Assert.assertFalse(actual.getHeader().isEmpty());
    Assert.assertEquals(1, actual.getHeader().size());
    Assert.assertEquals(expectedBasicAuth, actual.getHeader().get(SplunkCollectorHelper.HEADER_AUTHORIZATION));
    Assert.assertEquals(expectedContent, actual.getContent());
  }
}
