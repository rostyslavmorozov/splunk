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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Plugin inserts events into Splunk using Splunk Bulk API.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SplunkBatchSink.NAME)
@Description("Writes events to Splunk.")
public class SplunkBatchSink extends BatchSink<StructuredRecord, NullWritable, Text> {

  public static final String NAME = "Splunk";

  private final SplunkBatchSinkConfig config;

  public SplunkBatchSink(SplunkBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(collector, inputSchema);
    config.validateConnection(collector);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    Schema inputSchema = context.getInputSchema();
    config.validate(collector, inputSchema);
    config.validateConnection(collector);

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    if (inputSchema != null) {
      lineageRecorder.createExternalDataset(inputSchema);
      if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
        lineageRecorder.recordWrite("Write", "Write to Splunk",
                                    inputSchema.getFields().stream()
                                      .map(Schema.Field::getName)
                                      .collect(Collectors.toList()));
      }
    }

    context.addOutput(Output.of(config.referenceName, new SplunkOutputFormatProvider(config)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws IOException {
    String jsonString = StructuredRecordStringConverter.toJsonString(input);
    emitter.emit(new KeyValue<>(null, new Text(jsonString)));
  }
}
