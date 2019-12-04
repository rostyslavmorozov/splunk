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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.splunk.common.client.SplunkSearchClient;
import io.cdap.plugin.splunk.source.batch.SplunkMapToRecordTransformer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Splunk streaming source util. Which encapsulated spark classes from {@link SplunkStreamingSource},
 * so that validation does not fail loading spark classes.
 */
public class SplunkStreamingUtil {

  public static JavaDStream<StructuredRecord> getStream(SplunkStreamingSourceConfig config,
                                                        Schema schema,
                                                        StreamingContext streamingContext) {
    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();
    SplunkSearchClient splunkSearchClient = new SplunkSearchClient(config);
    String searchId = splunkSearchClient.prepareSearch();
    return jssc.receiverStream(new SplunkReceiver(config, searchId))
      .map(event -> SplunkMapToRecordTransformer.getStructuredRecord(event, schema));
  }
}
