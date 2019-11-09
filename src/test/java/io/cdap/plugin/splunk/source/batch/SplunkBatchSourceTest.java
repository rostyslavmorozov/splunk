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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splunk.HttpService;
import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests to verify configuration of {@link SplunkBatchSource}
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
public class SplunkBatchSourceTest extends HydratorTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SplunkBatchSourceTest.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  private static final String TOKEN_HEC = System.getProperty("splunk.test.token.hec");
  private static final String TOKEN_API = System.getProperty("splunk.test.token.api");
  private static final String URL_WRITE = System.getProperty("splunk.test.url.write");
  private static final String URL_READ = System.getProperty("splunk.test.url.read");
  private static final String INDEX = System.getProperty("splunk.test.index");

  private static final Gson GSON = new GsonBuilder().create();
  private static final Map<String, String> EXPECTED_EVENT_MAP = new HashMap<>();
  private static Service splunkClient;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setupTestClass() throws Exception {
    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
    try {
      Assume.assumeNotNull(TOKEN_HEC, TOKEN_API, URL_WRITE, URL_READ, INDEX);
    } catch (AssumptionViolatedException e) {
      LOG.warn("ETL tests are skipped. Please find the instructions on enabling it at " +
                 "SplunkBatchSourceTest javadoc.");
      throw e;
    }

    URL url = new URL(URL_WRITE);
    Map<String, Object> connectionArgs = new HashMap<>();
    connectionArgs.put("host", url.getHost());
    connectionArgs.put("token", "Splunk " + TOKEN_HEC);
    connectionArgs.put("port", url.getPort());
    connectionArgs.put("scheme", url.getProtocol());
    splunkClient = Service.connect(connectionArgs);
    createEvent();

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      SplunkBatchSource.class);
  }

  @Test
  public void testBatchSourceNormalSingleEventJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "json")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalAllEventsJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "json")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalAllIndexJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "json")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalSingleEventXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "xml")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalAllEventsXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "xml")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalAllIndexXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "xml")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalSingleEventCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "csv")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalAllEventsCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "csv")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceNormalAllIndexCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Normal")
      .put("outputFormat", "csv")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingSingleEventJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "json")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingAllEventsJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "json")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingAllIndexJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "json")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingSingleEventXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "xml")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingAllEventsXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "xml")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingAllIndexXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "xml")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingSingleEventCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "csv")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingAllEventsCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "csv")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceBlockingAllIndexCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", "Blocking")
      .put("outputFormat", "csv")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotSingleEventJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "json")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotAllEventsJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "json")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotAllIndexJson() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "json")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotSingleEventXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "xml")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotAllEventsXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "xml")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotAllIndexXml() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "xml")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotSingleEventCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "csv")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "1")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotAllEventsCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "csv")
      .put("searchString", String.format(
        "search index=\"%s\" event=\"%s\" | kvform", INDEX,
        EXPECTED_EVENT_MAP.get("event")))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  @Test
  public void testBatchSourceOneshotAllIndexCsv() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("authenticationType", "token")
      .put("token", TOKEN_API)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("url", URL_READ)
      .put("executionMode", SplunkBatchSourceConfig.ONESHOT_JOB)
      .put("outputFormat", "csv")
      .put("searchString", String.format("search index=\"%s\" | kvform", INDEX))
      .put("searchResultsCount", "0")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, SplunkBatchSource.NAME, "SplunkBatch");

    List<StructuredRecord> actual = outputRecords.stream()
      .filter(record -> EXPECTED_EVENT_MAP.get("event").equals(record.get("event")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());

    Assert.assertEquals(EXPECTED_EVENT_MAP.get("event"), actual.get(0).get("event"));
    Assert.assertEquals(EXPECTED_EVENT_MAP.get("testField"), actual.get(0).get("testField"));
  }

  private List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties,
                                                    String pluginName,
                                                    String applicationPrefix) throws Exception {
    ETLStage source = new ETLStage("SplunkReader", new ETLPlugin(
      pluginName, BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app(applicationPrefix + "_" + testName.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }

  private static void createEvent() throws IOException {
    long index = System.currentTimeMillis();
    EXPECTED_EVENT_MAP.put("event", String.format("test_event_%s", index));
    EXPECTED_EVENT_MAP.put("testField", String.format("test_val_%s", index));
    String event = GSON.toJson(EXPECTED_EVENT_MAP);

    RequestMessage request = new RequestMessage("POST");
    request.setContent(event);
    ResponseMessage response = splunkClient.send("/services/collector/raw", request);
    response.getContent().close();
  }
}
