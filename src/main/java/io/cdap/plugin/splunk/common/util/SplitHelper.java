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

import io.cdap.plugin.splunk.source.SplunkSourceConfig;
import io.cdap.plugin.splunk.source.batch.SplunkSplit;

/**
 * Builds Splunk Split.
 */
public class SplitHelper {

  private SplitHelper() {
  }

  public static long getTotalResults(long totalResults, long searchResultsCount) {
    if (searchResultsCount != 0L && searchResultsCount < totalResults) {
      return searchResultsCount;
    }
    return totalResults;
  }

  public static long getPartitionsCount(double totalResults) {
    return (long) Math.ceil(
      totalResults / (double) SplunkSourceConfig.PARTITION_MAX_SIZE);
  }

  public static SplunkSplit buildSplunkSplit(long totalResults, long partitionsCount,
                                             long partitionIndex, String searchId) {
    long offset = partitionIndex * SplunkSourceConfig.PARTITION_MAX_SIZE;
    long endPageCount = totalResults % SplunkSourceConfig.PARTITION_MAX_SIZE;
    long count;
    if (partitionIndex == partitionsCount - 1) {
      if (endPageCount != 0) {
        count = endPageCount;
      } else {
        count = SplunkSourceConfig.PARTITION_MAX_SIZE;
      }
    } else {
      count = SplunkSourceConfig.PARTITION_MAX_SIZE;
    }
    return new SplunkSplit(searchId, offset, count);
  }
}
