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

import com.splunk.Event;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Map;

/**
 * Transforms Splunk {@link Event} into {@link StructuredRecord}.
 */
public class SplunkMapToRecordTransformer {

  private final Schema schema;

  public SplunkMapToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  public StructuredRecord transform(Map<String, String> event) {
    return getStructuredRecord(event, schema);
  }

  public static StructuredRecord getStructuredRecord(Map<String, String> event, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    event.entrySet().stream()
      .filter(entry -> schema.getField(entry.getKey()) != null) // filter absent fields in the schema
      .forEach(entry -> builder.set(entry.getKey(), entry.getValue()));
    return builder.build();
  }
}
