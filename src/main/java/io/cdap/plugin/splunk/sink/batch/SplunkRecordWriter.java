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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splunk.RequestMessage;
import com.splunk.Service;
import io.cdap.plugin.splunk.common.exception.ConnectionTimeoutException;
import io.cdap.plugin.splunk.common.util.SearchHelper;
import io.cdap.plugin.splunk.common.util.SplunkCollectorHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Writes events into batches and submits them to Splunk Bulk job.
 * Accepts <code>null</code> as a key, and Text as a value.
 */
public class SplunkRecordWriter extends RecordWriter<NullWritable, Text> {

  private static final Gson GSON = new GsonBuilder().create();

  private final SplunkBatchSinkConfig config;
  private final Service splunkService;
  private final String basicAuth;
  private final String path;
  private final List<String> batchList = new ArrayList<>();

  public SplunkRecordWriter(TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    String configJson = conf.get(SplunkOutputFormatProvider.PROPERTY_CONFIG_JSON);

    this.config = GSON.fromJson(configJson, SplunkBatchSinkConfig.class);
    this.splunkService = Service.connect(config.getConnectionArguments());
    this.basicAuth = SplunkCollectorHelper.buildBasicAuth(config);
    this.path = SplunkCollectorHelper.buildPath(config, config.getEndpoint().toLowerCase());
  }

  @Override
  public void write(NullWritable key, Text text) {
    Map<Object, Object> eventMap = new HashMap<>(config.getEventMetadataMap());
    eventMap.put("event", new String(text.getBytes(), Charset.forName("UTF-8")));
    String event = GSON.toJson(eventMap);
    batchList.add(event);
    if (batchList.size() >= config.getBatchSize()) {
      submitBatch();
    }
  }

  @Override
  public void close(TaskAttemptContext context) {
    if (!batchList.isEmpty()) {
      submitBatch();
    }
  }

  private void submitBatch() {
    String content = String.join("", batchList);
    RequestMessage request = SplunkCollectorHelper
      .buildRequest(config, basicAuth, "POST", content);
    Retryer<Void> retryer = SearchHelper.buildRetryer(config);
    try {
      retryer.call(() -> SplunkCollectorHelper.sendRequest(splunkService, path, request));
    } catch (ExecutionException | RetryException e) {
      throw new ConnectionTimeoutException(
        String.format("Cannot create Splunk connection for path: '%s'", path), e);
    } finally {
      batchList.clear();
    }
  }
}
