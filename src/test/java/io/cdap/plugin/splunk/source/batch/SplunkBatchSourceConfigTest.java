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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.splunk.common.config.BaseSplunkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link SplunkBatchSourceConfig}
 */
public class SplunkBatchSourceConfigTest {

  private static final String MOCK_STAGE = "mockStage";

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

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidUrl() {
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
      "invalid",
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
      "schema");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_URL, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testUsernameEmpty() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "basic",
      "apiToken",
      "",
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
      "schema");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(BaseSplunkConfig.PROPERTY_USERNAME, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testPasswordEmpty() {
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
      "",
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
      "schema");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_PASSWORD, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testInvalidBasicAuthentication() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "basic",
      "apiToken",
      "invalid",
      60000,
      60000,
      3,
      60000,
      100,
      "http://localhost:8089",
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
      "schema");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(2, causeList.size());
    Assert.assertEquals(BaseSplunkConfig.PROPERTY_USERNAME, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_PASSWORD, collector.getValidationFailures().get(0)
      .getCauses().get(1).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testTokenEmpty() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "token",
      "",
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
      "schema");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(BaseSplunkConfig.PROPERTY_TOKEN, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testInvalidTokenAuthentication() {
    SplunkBatchSourceConfig config = new SplunkBatchSourceConfig(
      "reference",
      "token",
      "invalid",
      "",
      60000,
      60000,
      3,
      60000,
      100,
      "http://localhost:8089",
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
      "schema");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(BaseSplunkConfig.PROPERTY_TOKEN, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
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

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(2, causeList.size());
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_SEARCH_STRING, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_SEARCH_ID, collector.getValidationFailures().get(0)
      .getCauses().get(1).getAttribute(CauseAttributes.STAGE_CONFIG));
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

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_SEARCH_STRING, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
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

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(2, causeList.size());
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_EXECUTION_MODE, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_SEARCH_ID, collector.getValidationFailures().get(0)
      .getCauses().get(1).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testParseSchema() {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

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
      "Normal",
      "outputFormat",
      "",
      "searchId",
      0L,
      "earliestTime",
      "latestTime",
      "indexedEarliestTime",
      "indexedLatestTime",
      100L,
      expected.toString());

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Schema actual = config.parseSchema(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testParseSchemaInvalidJson() {
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
      "Normal",
      "outputFormat",
      "",
      "searchId",
      0L,
      "earliestTime",
      "latestTime",
      "indexedEarliestTime",
      "indexedLatestTime",
      100L,
      "{}");

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    try {
      config.parseSchema(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, collector.getValidationFailures().size());
      List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
      Assert.assertEquals(1, causeList.size());
      Assert.assertEquals(SplunkBatchSourceConfig.PROPERTY_SCHEMA, collector.getValidationFailures().get(0)
        .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }
}
