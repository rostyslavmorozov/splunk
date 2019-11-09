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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split used for mapreduce.
 */
public class SplunkSplit extends InputSplit implements Writable {

  private String searchId;
  private Long offset;
  private Long count;

  public SplunkSplit() {
    // For serialization
  }

  public SplunkSplit(String searchId, Long offset, Long count) {
    this.offset = offset;
    this.count = count;
    this.searchId = searchId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(searchId);
    out.writeLong(offset);
    out.writeLong(count);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    searchId = in.readUTF();
    offset = in.readLong();
    count = in.readLong();
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public String getSearchId() {
    return searchId;
  }

  public Long getOffset() {
    return offset;
  }

  public Long getCount() {
    return count;
  }
}
