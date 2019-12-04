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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.splunk.common.client.SplunkSearchClient;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link SchemaHelper}
 */
public class SchemaHelperTest {

  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void testParseSchema() {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Schema actual = SchemaHelper.getSchema(null, expected.toString(), collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testParseSchemaInvalidJson() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    SchemaHelper.getSchema(null, "{}", collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(SplunkSourceConfig.PROPERTY_SCHEMA, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testGetSchemaFromSplunk() throws IOException {
    SplunkSearchClient client = Mockito.mock(SplunkSearchClient.class);

    List<Map<String, String>> sample = new ArrayList<>();
    sample.add(ImmutableMap.of("field1", "value1", "field2", "value2"));
    sample.add(ImmutableMap.of("field1", "value1", "field3", "value3"));

    Mockito.when(client.getSample()).thenReturn(sample);

    Schema expected = Schema.recordOf(
      "event",
      Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("field2", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("field3", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Schema actual = SchemaHelper.getSchema(client, null);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }
}
