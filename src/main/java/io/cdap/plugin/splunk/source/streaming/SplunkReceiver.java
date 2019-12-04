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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cdap.plugin.splunk.common.client.SplunkSearchIterator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Implementation of Spark receiver to receive Splunk events
 */
public class SplunkReceiver extends Receiver<Map<String, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(SplunkReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "splunk_api_listener";

  private final SplunkStreamingSourceConfig config;
  private final String searchId;

  public SplunkReceiver(SplunkStreamingSourceConfig config, String searchId) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.config = config;
    this.searchId = searchId;
  }

  @Override
  public void onStart() {
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat(RECEIVER_THREAD_NAME + "-%d")
      .build();

    Executors.newSingleThreadExecutor(namedThreadFactory).submit(this::receive);
  }

  @Override
  public void onStop() {
    // There is nothing we can do here as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  private void receive() {
    try {
      long allRecordsCount = 0L;
      SplunkSearchIterator searchIterator = new SplunkSearchIterator(config, searchId, 0L, allRecordsCount);

      while (!isStopped()) {
        if (searchIterator.hasNext()) {
          store(searchIterator.next());
        } else {
          long interval = config.getPollInterval();
          LOG.debug(String.format("Waiting for '%d' milliseconds to pull.", interval));
          Thread.sleep(interval);

          // reload current iterator
          long iteratorPosition = searchIterator.getIteratorPosition();
          searchIterator = new SplunkSearchIterator(config, searchId, iteratorPosition, allRecordsCount);
          searchIterator.setIteratorPosition(iteratorPosition);
        }
      }
    } catch (Exception e) {
      String errorMessage = "Exception while receiving messages from Splunk";
      throw new RuntimeException(errorMessage, e);
    }
  }
}
