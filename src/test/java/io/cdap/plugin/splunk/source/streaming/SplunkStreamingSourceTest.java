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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import com.splunk.Service;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.plugin.splunk.etl.BaseSplunkTest;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests to verify configuration of {@link SplunkStreamingSource}
 * <p>
 * By default all tests will be skipped, since Splunk credentials are needed.
 * <p>
 * Instructions to enable the tests:
 * 1. Create/use existing Splunk account.
 * 2. Create HEC Token for writing with separate index for testing.
 * 3. Create API Token for reading.
 * 4. Run the tests using the command below:
 * <p>
 * mvn clean test
 * -Dsplunk.test.token.hec=
 * -Dsplunk.test.token.api=
 * -Dsplunk.test.url.write=
 * -Dsplunk.test.url.read=
 * -Dsplunk.test.index=
 */
public class SplunkStreamingSourceTest extends BaseSplunkTest {

  private static final Gson GSON = new GsonBuilder().create();
  private static Service splunkClient;

  private SparkManager programManager;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    splunkClient = buildSplunkClient(URL_WRITE, "Splunk " + TOKEN_HEC);
  }

  @After
  public void tearDown() throws Exception {
    if (programManager != null) {
      programManager.stop();
      programManager.waitForStopped(10, TimeUnit.SECONDS);
      programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testSplunkStreamingSource() throws Exception {
    ImmutableMap<String, String> properties = getBasicConfig()
      .put("executionMode", "Normal")
      .put("outputFormat", "json")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    String outputTable = testName.getMethodName() + "_out";
    programManager = deploySourcePlugin(properties, outputTable);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);

    // Wait for Spark to start up the job by checking if receiver thread exists
    Awaitility.await()
      .atMost(60, TimeUnit.SECONDS)
      .pollInterval(100, TimeUnit.MILLISECONDS)
      .untilAsserted(() -> Assert.assertTrue(
        "Splunk receiver thread has not started", this.hasReceiverThreadStarted()));

    Map<String, String> expectedEventMap = new HashMap<>();
    createEvent(expectedEventMap);

    waitForRecords(outputTable, expectedEventMap);

    DataSetManager<Table> outputManager = getDataset(outputTable);
    MockSink.clear(outputManager);

    expectedEventMap = new HashMap<>();
    createEvent(expectedEventMap);

    waitForRecords(outputTable, expectedEventMap);
  }

  private ImmutableMap.Builder<String, String> getBasicConfig() {
    return ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("pollInterval", "100");
  }

  private SparkManager deploySourcePlugin(Map<String, String> properties, String outputTable) throws Exception {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      SplunkStreamingSource.NAME, StreamingSource.PLUGIN_TYPE, properties, null));

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputTable));

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("1s")
      .setStopGracefully(true)
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("SplunkSource" + "_" + testName.getMethodName());
    ApplicationManager applicationManager = deployApplication(pipelineId, new AppRequest<>(APP_STREAMS, etlConfig));

    return applicationManager.getSparkManager(DataStreamsSparkLauncher.NAME);
  }

  /**
   * Checks if Spark Receiver has started by checking if spark_splunk_receiver thread started.
   * This usually happens in ~15 seconds after pipeline status changes to "running".
   * <p>
   * We need this to understand when Spark starts listening for events. So we can submit data to
   * Splunk and receive data.
   *
   * @return true if receiver thread started
   */
  private boolean hasReceiverThreadStarted() {
    Set<String> resultSet = Thread.getAllStackTraces().keySet()
      .stream()
      .map(Thread::getName)
      .filter(name -> name.startsWith("splunk_api_listener"))
      .collect(Collectors.toSet());

    return !resultSet.isEmpty();
  }

  private static void createEvent(Map<String, String> expectedEventMap) throws IOException {
    long index = System.currentTimeMillis();
    expectedEventMap.put("event", String.format("test_event_%s", index));
    expectedEventMap.put("testField", String.format("test_val_%s", index));
    String event = GSON.toJson(expectedEventMap);

    RequestMessage request = new RequestMessage("POST");
    request.setContent(event);
    ResponseMessage response = splunkClient.send("/services/collector/raw", request);
    response.getContent().close();
  }

  private void waitForRecords(String outputTable, Map<String, String> expectedEventMap) throws Exception {
    DataSetManager<Table> outputManager = getDataset(outputTable);
    Awaitility.await()
      .atMost(60, TimeUnit.SECONDS)
      .untilAsserted(() -> Assert.assertEquals(
        1, MockSink.readOutput(outputManager).size()));

    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    assertEventResult(expectedEventMap, output);
  }

  private void assertEventResult(Map<String, String> expectedEventMap, List<StructuredRecord> output) {
    List<StructuredRecord> actual = output.stream()
      .filter(record -> expectedEventMap.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(expectedEventMap.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(expectedEventMap.get("testField"), actual.get(0).get("testField"));
  }
}
