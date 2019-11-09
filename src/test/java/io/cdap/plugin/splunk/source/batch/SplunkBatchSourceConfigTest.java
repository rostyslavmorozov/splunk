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

import io.cdap.cdap.etl.api.FailureCollector;
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
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "basic",
      "apiToken",
      "userName",
      60000,
      60000,
      3,
      60000,
      100,
      "https://localhost:8089",
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
      "schema") {
      @Override
      void validateConnection(FailureCollector collector) {
      }
    };

    assertValidationSucceed(config);
  }

  @Test
  public void testInvalidUrl() {
    SplunkBatchSourceConfig config = SplunkBatchSourceConfigBuilder.CONFIG;

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_URL));
  }

  @Test
  public void testUsernameEmpty() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setUsername("")
        .setUrl("https://localhost:8089")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_USERNAME));
  }

  @Test
  public void testPasswordEmpty() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setUrl("https://localhost:8089")
        .setPassword("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_PASSWORD));
  }

  @Test
  public void testInvalidBasicAuthentication() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setUsername("invalid")
        .setUrl("https://localhost:8089")
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
        .setUrl("https://localhost:8089")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testInvalidTokenAuthentication() {
    SplunkBatchSourceConfig config =
      new SplunkBatchSourceConfigBuilder(SplunkBatchSourceConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("invalid")
        .setUrl("https://localhost:8089")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testSearchStringAndSearchIdEmpty() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "basic",
      "apiToken",
      "userName",
      60000,
      60000,
      3,
      60000,
      100,
      "https://localhost:8089",
      "password",
      "executionMode",
      "outputFormat",
      "",
      "",
      0L,
      "earliestTime",
      "latestTime",
      "indexedEarliestTime",
      "indexedLatestTime",
      100L,
      "schema") {
      @Override
      void validateConnection(FailureCollector collector) {
      }
    };

    assertValidationFailed(config, Arrays.asList(SplunkBatchSourceConfig.PROPERTY_SEARCH_STRING,
                                                 SplunkBatchSourceConfig.PROPERTY_SEARCH_ID));
  }

  @Test
  public void testSearchStringIsNotValid() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "basic",
      "apiToken",
      "userName",
      60000,
      60000,
      3,
      60000,
      100,
      "https://localhost:8089",
      "password",
      "executionMode",
      "outputFormat",
      "notValid",
      "",
      0L,
      "earliestTime",
      "latestTime",
      "indexedEarliestTime",
      "indexedLatestTime",
      100L,
      "schema") {
      @Override
      void validateConnection(FailureCollector collector) {
      }
    };

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSourceConfig.PROPERTY_SEARCH_STRING));
  }

  @Test
  public void testSearchIdInOneshot() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "basic",
      "apiToken",
      "userName",
      60000,
      60000,
      3,
      60000,
      100,
      "https://localhost:8089",
      "password",
      SplunkBatchSourceConfig.ONESHOT_JOB,
      "outputFormat",
      "",
      "searchId",
      0L,
      "earliestTime",
      "latestTime",
      "indexedEarliestTime",
      "indexedLatestTime",
      100L,
      "schema") {
      @Override
      void validateConnection(FailureCollector collector) {
      }
    };

    assertValidationFailed(config, Arrays.asList(SplunkBatchSourceConfig.PROPERTY_EXECUTION_MODE,
                                                 SplunkBatchSourceConfig.PROPERTY_SEARCH_ID));
  }
}
