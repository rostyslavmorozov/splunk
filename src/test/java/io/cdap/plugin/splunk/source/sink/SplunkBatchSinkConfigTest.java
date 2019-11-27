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

package io.cdap.plugin.splunk.source.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.splunk.common.config.BaseSplunkConfig;
import io.cdap.plugin.splunk.common.config.BaseSplunkValidationTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for {@link SplunkBatchSinkConfig}
 */
public class SplunkBatchSinkConfigTest extends BaseSplunkValidationTest {

  private static final String MOCK_STAGE = "mockStage";

  private static final Schema VALID_SCHEMA = Schema.recordOf(
    "test",
    Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
  );

  @Test
  public void testValidate() {
    SplunkBatchSinkConfig config = SplunkBatchSinkConfigBuilder.CONFIG;
    assertValidationSucceed(config);
  }

  @Test
  public void testValidateQueryStringAuthTokenEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("query")
        .setToken("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN), true);
  }

  @Test
  public void testValidateTokenAuthTokenEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setAuthenticationType("token")
        .setToken("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN), true);
  }

  @Test
  public void testValidateBasicAuthUserNameEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUsername("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_USERNAME), true);
  }

  @Test
  public void testValidateBasicAuthTokenEmpty() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setToken("")
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN), true);
  }

  @Test
  public void testInvalidBasicAuthentication() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUrl("http://invalid.host.test.localhost:8088")
        .build();

    assertValidationFailed(config, Arrays.asList(SplunkBatchSinkConfig.PROPERTY_USERNAME,
                                                 SplunkBatchSinkConfig.PROPERTY_TOKEN),
                           true);
  }

  @Test
  public void testInvalidTokenAuthentication() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setUrl("http://invalid.host.test.localhost:8088")
        .setAuthenticationType("token")
        .setUsername(null)
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN), true);
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

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_TOKEN), true);
  }

  @Test
  public void testValidateSchemaNull() {
    SplunkBatchSinkConfig config = SplunkBatchSinkConfigBuilder.CONFIG;
    assertValidationFailed(config, Collections.singletonList(BaseSplunkConfig.PROPERTY_SCHEMA), null);
  }

  @Test
  public void testValidateSchemaFieldsNull() {
    Schema schema = Schema.recordOf("test");

    SplunkBatchSinkConfig config = SplunkBatchSinkConfigBuilder.CONFIG;
    assertValidationFailed(config, Collections.singletonList(BaseSplunkConfig.PROPERTY_SCHEMA), schema);
  }

  @Test
  public void testValidateChannelNullForRawEndpoint() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setEndpoint(SplunkBatchSinkConfig.ENPOINT_RAW)
        .setChannelIdentifierHeader(null)
        .build();

    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_CHANNEL_IDENTIFIER_HEADER));
  }

  @Test
  public void testValidateChannelEmptyForRawEndpoint() {
    SplunkBatchSinkConfig config =
      new SplunkBatchSinkConfigBuilder(SplunkBatchSinkConfigBuilder.CONFIG)
        .setEndpoint(SplunkBatchSinkConfig.ENPOINT_RAW)
        .setChannelIdentifierHeader("")
        .build();
    assertValidationFailed(config, Collections.singletonList(SplunkBatchSinkConfig.PROPERTY_CHANNEL_IDENTIFIER_HEADER));
  }

  private static void assertValidationSucceed(SplunkBatchSinkConfig config) {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector, VALID_SCHEMA);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  private static void assertValidationFailed(SplunkBatchSinkConfig config, List<String> paramNames) {
    assertValidationFailed(config, paramNames, VALID_SCHEMA);
  }

  private static void assertValidationFailed(SplunkBatchSinkConfig config, List<String> paramNames, Schema schema) {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, schema);

    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = failureCollector.getValidationFailures().get(0).getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(paramNames.size(), causeList.size());
    IntStream.range(0, paramNames.size())
      .forEachOrdered(i -> Assert.assertEquals(
        paramNames.get(i), causeList.get(i).getAttribute(CauseAttributes.STAGE_CONFIG)));
  }
}
