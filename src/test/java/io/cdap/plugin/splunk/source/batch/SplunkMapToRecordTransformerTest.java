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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for {@link SplunkMapToRecordTransformer}
 */
public class SplunkMapToRecordTransformerTest {

  @Test
  public void testTransform() {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("test_field_1", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("test_field_2", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("test_field_5", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    Map<String, String> event = ImmutableMap.of("test_field_1", "test_value_1",
                                                "test_field_2", "test_value_2",
                                                "test_field_3", "test_value_3",
                                                "test_field_4", "test_value_4");

    Map<String, String> expected = ImmutableMap.of("test_field_1", "test_value_1",
                                                   "test_field_2", "test_value_2");

    SplunkMapToRecordTransformer transformer = new SplunkMapToRecordTransformer(schema);
    StructuredRecord actual = transformer.transform(event);

    Assert.assertEquals(expected.get("test_field_1"), actual.get("test_field_1"));
    Assert.assertEquals(expected.get("test_field_2"), actual.get("test_field_2"));
    Assert.assertNull(actual.get("test_field_5"));
  }
}
