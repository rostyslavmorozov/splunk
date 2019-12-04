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
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SplitHelper}
 */
public class SplitHelperTest {

  @Test
  public void testTotalResultsRequestedAll() {
    long expected = 10L;
    long actual = SplitHelper.getTotalResults(expected, 0L);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTotalResultsLessThanRequested() {
    long expected = 10L;
    long actual = SplitHelper.getTotalResults(expected, 100L);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTotalResultsGreaterThanRequested() {
    long expected = 10L;
    long actual = SplitHelper.getTotalResults(100L, expected);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPartitionsCountTotalZero() {
    long actual = SplitHelper.getPartitionsCount(0L);
    Assert.assertEquals(0L, actual);
  }

  @Test
  public void testPartitionsCountTotalLessThanPartitionSize() {
    long actual = SplitHelper.getPartitionsCount(100L);
    Assert.assertEquals(1L, actual);
  }

  @Test
  public void testPartitionsCountTotalEqualsPartitionSize() {
    long actual = SplitHelper.getPartitionsCount(SplunkSourceConfig.PARTITION_MAX_SIZE);
    Assert.assertEquals(1L, actual);
  }

  @Test
  public void testPartitionsCountTotalGreaterThanPartitionSize() {
    long actual = SplitHelper.getPartitionsCount(20001L);
    Assert.assertEquals(2L, actual);
  }

  @Test
  public void testSplitNotLastPartitionEndPageNotZero() {
    Long expectedOffset = 0L;

    long totalResults = 40001L;
    long partitionsCount = 3L;
    long partitionIndex = 0L;
    String searchId = "";

    SplunkSplit actual = SplitHelper.buildSplunkSplit(
      totalResults, partitionsCount, partitionIndex, searchId);

    Assert.assertNotNull(actual);

    Assert.assertEquals(expectedOffset, actual.getOffset());
    Assert.assertEquals(SplunkSourceConfig.PARTITION_MAX_SIZE, actual.getCount());
    Assert.assertEquals(searchId, actual.getSearchId());
  }

  @Test
  public void testSplitLastPartitionEndPageNotZero() {
    Long expectedOffset = 40000L;
    Long expectedCount = 1L;

    long totalResults = 40001L;
    long partitionsCount = 3L;
    long partitionIndex = 2L;
    String searchId = "";

    SplunkSplit actual = SplitHelper.buildSplunkSplit(
      totalResults, partitionsCount, partitionIndex, searchId);

    Assert.assertNotNull(actual);

    Assert.assertEquals(expectedOffset, actual.getOffset());
    Assert.assertEquals(expectedCount, actual.getCount());
    Assert.assertEquals(searchId, actual.getSearchId());
  }

  @Test
  public void testSplitNotLastPartitionEndPageZero() {
    Long expectedOffset = 0L;

    long totalResults = 40000L;
    long partitionsCount = 2L;
    long partitionIndex = 0L;
    String searchId = "";

    SplunkSplit actual = SplitHelper.buildSplunkSplit(
      totalResults, partitionsCount, partitionIndex, searchId);

    Assert.assertNotNull(actual);

    Assert.assertEquals(expectedOffset, actual.getOffset());
    Assert.assertEquals(SplunkSourceConfig.PARTITION_MAX_SIZE, actual.getCount());
    Assert.assertEquals(searchId, actual.getSearchId());
  }

  @Test
  public void testSplitLastPartitionEndPageZero() {
    Long expectedOffset = 20000L;

    long totalResults = 40000L;
    long partitionsCount = 2L;
    long partitionIndex = 1L;
    String searchId = "";

    SplunkSplit actual = SplitHelper.buildSplunkSplit(
      totalResults, partitionsCount, partitionIndex, searchId);

    Assert.assertNotNull(actual);

    Assert.assertEquals(expectedOffset, actual.getOffset());
    Assert.assertEquals(SplunkSourceConfig.PARTITION_MAX_SIZE, actual.getCount());
    Assert.assertEquals(searchId, actual.getSearchId());
  }
}
