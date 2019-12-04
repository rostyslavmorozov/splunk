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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.splunk.ValidationAssertions;
import io.cdap.plugin.splunk.common.config.BaseSplunkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link SplunkBatchSinkConfig}
 */
public class SplunkBatchSinkConfigTest {

  private static final String MOCK_STAGE = "mockStage";

  private static final Schema VALID_SCHEMA = Schema.recordOf(
    "test",
    Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
  );

  @Test
  public void testValidate() {
    SplunkBatchSinkConfig config = SplunkBatchSinkConfigBuilder.CONFIG;

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector, VALID_SCHEMA);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateQueryStringAuthTokenEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("query")
        .setToken("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testValidateTokenAuthTokenEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testValidateBasicAuthUserNameEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUsername("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_USERNAME));
  }

  @Test
  public void testValidateBasicAuthTokenEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setToken("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testInvalidBasicAuthentication() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUrl("http://invalid.host.test.localhost:8088")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Arrays.asList(SplunkBatchSinkConfig.PROPERTY_USERNAME,
                                      SplunkBatchSinkConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testInvalidTokenAuthentication() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUrl("http://invalid.host.test.localhost:8088")
        .setAuthenticationType("token")
        .setUsername(null)
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testInvalidQueryStringAuthentication() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUrl("http://invalid.host.test.localhost:8088")
        .setAuthenticationType("query")
        .setToken("invalid")
        .setUsername(null)
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN));
  }

  @Test
  public void testValidateSchemaNull() {
    SplunkBatchSinkConfig config = SplunkBatchSinkConfigBuilder.CONFIG;
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, null);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(BaseSplunkConfig.PROPERTY_SCHEMA));
  }

  @Test
  public void testValidateSchemaFieldsNull() {
    Schema schema = Schema.recordOf("test");

    SplunkBatchSinkConfig config = SplunkBatchSinkConfigBuilder.CONFIG;
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, schema);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(BaseSplunkConfig.PROPERTY_SCHEMA));
  }

  @Test
  public void testValidateChannelNullForRawEndpoint() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setEndpoint(SplunkBatchSinkConfig.ENPOINT_RAW)
        .setChannelIdentifierHeader(null)
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_CHANNEL_IDENTIFIER_HEADER));
  }

  @Test
  public void testValidateChannelEmptyForRawEndpoint() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setEndpoint(SplunkBatchSinkConfig.ENPOINT_RAW)
        .setChannelIdentifierHeader("")
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);

    ValidationAssertions.assertValidationFailed(
      failureCollector, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_CHANNEL_IDENTIFIER_HEADER));
  }
}
