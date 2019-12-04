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

package io.cdap.plugin.splunk.common.client;

import com.google.common.collect.ImmutableMap;
import com.splunk.Event;
import com.splunk.ResultsReader;
import io.cdap.plugin.splunk.source.SplunkSourceConfig;
import io.cdap.plugin.splunk.source.SplunkSourceConfigBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link SplunkSearchIterator}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultsReader.class)
@PowerMockIgnore({"javax.net.ssl.*"})
public class SplunkSearchIteratorTest {

  private static final String SEARCH_ID = "searchId";
  private static final long OFFSET = 0L;

  @Test
  public void testSearchIterator() {
    Map<String, String> expectedEvent1 = ImmutableMap.of("key1", "event1");

    Map<String, String> event1 = ImmutableMap.of("!k^e*y()1{}", "event1");
    Map<String, String> event2 = ImmutableMap.of("key1", "event2");
    Map<String, String> event3 = ImmutableMap.of("key1", "event3");
    List<Event> events = new ArrayList<>();
    addEvent(events, event1);
    addEvent(events, event2);
    addEvent(events, event3);

    InputStream stream = Mockito.mock(InputStream.class);
    ResultsReader resultsReader = PowerMockito.mock(ResultsReader.class);

    Mockito.when(resultsReader.iterator()).thenReturn(events.iterator());

    SplunkSearchIterator searchIterator = new SplunkSearchIterator(
      null, SplunkSourceConfigBuilder.CONFIG, SEARCH_ID, OFFSET,
      SplunkSourceConfig.PARTITION_MAX_SIZE) {
      @Override
      ResultsReader getResultsReader(InputStream stream, String outputFormat) {
        return resultsReader;
      }

      @Override
      InputStream getStreamResults(SplunkSourceConfig config,
                                   String searchId, long offset, Long count) {
        return stream;
      }
    };

    boolean actual = searchIterator.hasNext();
    Map<String, String> next = searchIterator.next();
    Assert.assertTrue(actual);
    Assert.assertEquals(expectedEvent1, next);

    actual = searchIterator.hasNext();
    next = searchIterator.next();
    Assert.assertTrue(actual);
    Assert.assertEquals(event2, next);

    actual = searchIterator.hasNext();
    next = searchIterator.next();
    Assert.assertTrue(actual);
    Assert.assertEquals(event3, next);

    actual = searchIterator.hasNext();
    Assert.assertFalse(actual);
  }

  private void addEvent(List events, Map<String, String> event) {
    events.add(event);
  }
}
