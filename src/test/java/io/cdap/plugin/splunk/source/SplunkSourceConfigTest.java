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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.splunk.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link SplunkSourceConfig}
 */
public class SplunkSourceConfigTest {

  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void testValidate() {
    SplunkSourceConfig config = SplunkSourceConfigBuilder.CONFIG;

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testUsernameEmpty() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setUsername("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkSourceConfig.PROPERTY_USERNAME));
  }

  @Test
  public void testPasswordEmpty() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setPassword("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkSourceConfig.PROPERTY_PASSWORD));
  }

  @Test
  public void testInvalidBasicAuthentication() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setUsername("invalid")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Arrays.asList(SplunkSourceConfig.PROPERTY_USERNAME,
                                      SplunkSourceConfig.PROPERTY_PASSWORD));
  }

  @Test
  public void testTokenEmpty() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkSourceConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testInvalidTokenAuthentication() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("invalid")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkSourceConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testSearchStringAndSearchIdEmpty() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setSearchString("")
        .setSearchId("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Arrays.asList(SplunkSourceConfig.PROPERTY_SEARCH_STRING,
                                      SplunkSourceConfig.PROPERTY_SEARCH_ID));
  }

  @Test
  public void testSearchStringIsNotValid() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setSearchString("notValid")
        .setSearchId("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkSourceConfig.PROPERTY_SEARCH_STRING));
  }

  @Test
  public void testSearchIdInOneshot() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setExecutionMode(SplunkSourceConfig.ONESHOT_JOB)
        .setSearchString("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Arrays.asList(SplunkSourceConfig.PROPERTY_EXECUTION_MODE,
                                      SplunkSourceConfig.PROPERTY_SEARCH_ID));
  }
}
