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

package io.cdap.plugin.splunk.source.sink;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An OutputFormat that sends the output of a Hadoop job to the Splunk record writer.
 */
public class SplunkOutputFormat extends OutputFormat<NullWritable, Text> {

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext context) {
    return new SplunkRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    // no-op
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {
        // no-op
      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {
        // no-op
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {
        // no-op
      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {
        // no-op
      }
    };
  }
}
