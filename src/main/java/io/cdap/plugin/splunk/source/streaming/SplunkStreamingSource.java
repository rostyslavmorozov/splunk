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

package io.cdap.plugin.splunk.source.streaming;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.splunk.common.client.SplunkSearchClient;
import io.cdap.plugin.splunk.common.util.SchemaHelper;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Returns records in pseudo-realtime created by Splunk.
 * Polls an event endpoint and outputs records for each url response.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SplunkStreamingSource.NAME)
@Description("Streams data updates from Splunk.")
public class SplunkStreamingSource extends StreamingSource<StructuredRecord> {

  public static final String NAME = "Splunk";

  private final SplunkStreamingSourceConfig config;

  public SplunkStreamingSource(SplunkStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    config.validateConnection(failureCollector);
    SplunkSearchClient splunkClient = new SplunkSearchClient(config);
    Schema schema = SchemaHelper.getSchema(splunkClient, config.getSchema(), failureCollector);
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    config.validateConnection(failureCollector);
    SplunkSearchClient splunkClient = new SplunkSearchClient(config);
    Schema schema = SchemaHelper.getSchema(splunkClient, config.getSchema(), failureCollector);
    failureCollector.getOrThrowException();

    return SplunkStreamingUtil.getStream(config, schema, context);
  }
}
