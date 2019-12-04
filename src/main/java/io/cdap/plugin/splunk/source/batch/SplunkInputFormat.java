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
import io.cdap.plugin.splunk.common.util.SplitHelper;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Input format class which generates input splits for each given object
 * and initializes appropriate record reader.
 */
public class SplunkInputFormat extends InputFormat {

  private static final Gson GSON = new GsonBuilder().create();

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration configuration = jobContext.getConfiguration();
    SplunkSourceConfig config =
      GSON.fromJson(configuration.get(SplunkInputFormatProvider.PROPERTY_CONFIG_JSON),
                    SplunkSourceConfig.class);

    SplunkSearchClient splunkClient = new SplunkSearchClient(config);
    long totalResults = SplitHelper.getTotalResults(
      splunkClient.getTotalResults(), config.getSearchResultsCount());
    long partitionsCount = SplitHelper.getPartitionsCount(totalResults);

    String searchId = splunkClient.prepareSearch();
    return LongStream.range(0, partitionsCount)
      .boxed()
      .map(partitionIndex -> SplitHelper.buildSplunkSplit(
        totalResults, partitionsCount, partitionIndex, searchId))
      .collect(Collectors.toList());
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit,
                                         TaskAttemptContext context) {
    return new SplunkRecordReader();
  }
}
