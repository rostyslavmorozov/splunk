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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.splunk.common.client.SplunkSearchClient;
import io.cdap.plugin.splunk.common.client.SplunkSearchIterator;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * RecordReader implementation, which reads object from Splunk.
 */
public class SplunkRecordReader extends RecordReader<NullWritable, Map<String, String>> {

  private static final Gson GSON = new GsonBuilder().create();

  private SplunkSearchIterator searchIterator;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) {
    SplunkSplit splunkSplit = (SplunkSplit) inputSplit;
    String searchId = splunkSplit.getSearchId();
    Long offset = splunkSplit.getOffset();
    Long count = splunkSplit.getCount();

    Configuration conf = context.getConfiguration();
    String configJson = conf.get(SplunkInputFormatProvider.PROPERTY_CONFIG_JSON);
    SplunkSourceConfig config = GSON.fromJson(configJson, SplunkSourceConfig.class);
    SplunkSearchClient splunkClient = new SplunkSearchClient(config);
    searchIterator = splunkClient.buildSearchIterator(searchId, offset, count);
  }

  @Override
  public boolean nextKeyValue() {
    return searchIterator.hasNext();
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public Map<String, String> getCurrentValue() {
    return searchIterator.next();
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (searchIterator != null) {
      searchIterator.close();
    }
  }
}
