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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.splunk.common.client.SplunkSearchClient;
import io.cdap.plugin.splunk.common.exception.SchemaParseException;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves schema.
 */
public class SchemaHelper {

  private SchemaHelper() {
  }

  public static Schema getSchema(SplunkSearchClient splunkClient, String schema,
                                 FailureCollector failureCollector) {
    try {
      return getSchema(splunkClient, schema);
    } catch (SchemaParseException e) {
      failureCollector.addFailure("Unable to retrieve output schema.",
                                  null)
        .withConfigProperty(SplunkSourceConfig.PROPERTY_SCHEMA);
      return null;
    }
  }

  public static Schema getSchema(SplunkSearchClient splunkClient, String schema) {
    if (!Strings.isNullOrEmpty(schema)) {
      try {
        return Schema.parseJson(schema);
      } catch (IOException | IllegalStateException e) {
        throw new SchemaParseException(e);
      }
    }
    try {
      List<Map<String, String>> result = splunkClient.getSample();
      List<Schema.Field> fields = result.stream()
        .flatMap(entity -> entity.keySet().stream())
        .distinct()
        .map(fieldKey -> Schema.Field.of(fieldKey, Schema.nullableOf(Schema.of(Schema.Type.STRING))))
        .collect(Collectors.toList());
      return Schema.recordOf("event", fields);
    } catch (IOException e) {
      throw new SchemaParseException(e);
    }
  }
}
