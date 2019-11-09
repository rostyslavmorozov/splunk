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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.splunk.common.util.SchemaHelper;
import org.apache.hadoop.io.NullWritable;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Source plugin to read data from Splunk.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SplunkBatchSource.NAME)
@Description("Read data from Splunk.")
public class SplunkBatchSource extends BatchSource<NullWritable, Map<String, String>, StructuredRecord> {

  public static final String NAME = "Splunk";

  private final SplunkBatchSourceConfig config;

  public SplunkBatchSource(SplunkBatchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Schema schema = SchemaHelper.getSchema(config, failureCollector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Schema schema = SchemaHelper.getSchema(config, failureCollector);
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", "Read from Splunk",
                               Preconditions.checkNotNull(schema.getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    context.setInput(Input.of(config.referenceName, new SplunkInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<NullWritable, Map<String, String>> input,
                        Emitter<StructuredRecord> emitter) {
    Map<String, String> entity = input.getValue();
    Schema schema = SchemaHelper.getSchema(config, null);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    entity.entrySet().stream()
      .filter(entry -> schema.getField(entry.getKey()) != null) // filter absent fields in the schema
      .forEach(entry -> builder.set(entry.getKey(), entry.getValue()));
    StructuredRecord record = builder.build();
    emitter.emit(record);
  }
}
