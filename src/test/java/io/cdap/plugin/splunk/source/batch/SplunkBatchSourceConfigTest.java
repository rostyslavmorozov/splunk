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

package io.cdap.plugin.splunk.source.batch;

import io.cdap.plugin.splunk.common.config.BaseSplunkValidationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link SplunkBatchSourceConfig}
 */
public class SplunkBatchSourceConfigTest extends BaseSplunkValidationTest {

  @Test
  public void testValidate() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setMockConnection(true)
        .build();

    assertValidationSucceed(config);
  }

  @Test
  public void testUsernameEmpty() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setUsername("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_USERNAME));
  }

  @Test
  public void testPasswordEmpty() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setPassword("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_PASSWORD));
  }

  @Test
  public void testInvalidBasicAuthentication() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setUsername("invalid")
        .build();

    assertValidationFailed(config, Arrays.asList(SplunkBatchSourceConfig.PROPERTY_USERNAME,
                                                 SplunkBatchSourceConfig.PROPERTY_PASSWORD));
  }

  @Test
  public void testTokenEmpty() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testInvalidTokenAuthentication() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("invalid")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testSearchStringAndSearchIdEmpty() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setSearchString("")
        .setSearchId("")
        .setMockConnection(true)
        .build();

    assertValidationFailed(config, Arrays.asList(SplunkBatchSourceConfig.PROPERTY_SEARCH_STRING,
                                                 SplunkBatchSourceConfig.PROPERTY_SEARCH_ID));
  }

  @Test
  public void testSearchStringIsNotValid() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setSearchString("notValid")
        .setSearchId("")
        .setMockConnection(true)
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_SEARCH_STRING));
  }

  @Test
  public void testSearchIdInOneshot() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setExecutionMode(SplunkBatchSourceConfig.ONESHOT_JOB)
        .setSearchString("")
        .setMockConnection(true)
        .build();

    assertValidationFailed(config, Arrays.asList(SplunkBatchSourceConfig.PROPERTY_EXECUTION_MODE,
                                                 SplunkBatchSourceConfig.PROPERTY_SEARCH_ID));
  }
}
