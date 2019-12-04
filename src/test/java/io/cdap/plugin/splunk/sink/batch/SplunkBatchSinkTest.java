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

package io.cdap.plugin.splunk.sink.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.splunk.ResultsReaderJson;
import com.splunk.Service;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.splunk.etl.BaseSplunkTest;
import io.cdap.plugin.splunk.source.batch.SplunkBatchSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests to verify configuration of {@link SplunkBatchSink}
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
public class SplunkBatchSinkTest extends BaseSplunkTest {

  private static Service splunkClient;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    splunkClient = buildSplunkClient(URL_READ, "Bearer " + TOKEN_API);
  }

  @Test
  public void testBatchSink() throws Exception {
    ImmutableMap<String, String> properties = getBasicConfig()
      .put("batchSize", "1")
      .build();

    long index = System.currentTimeMillis();

    Schema schema = Schema.recordOf(
      "output",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ShippingLatitude", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("ShippingLongitude", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("testField", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord record1 = StructuredRecord.builder(schema)
      .set("Name", "testUpdateAccount1")
      .set("NumberOfEmployees", 6)
      .set("ShippingLatitude", 50.4501)
      .set("ShippingLongitude", 30.5234)
      .set("testField", String.format("test_val_%s", index))
      .build();
    StructuredRecord record2 = StructuredRecord.builder(schema)
      .set("Name", "testUpdateAccount2")
      .set("NumberOfEmployees", 1)
      .set("ShippingLatitude", 37.4220)
      .set("ShippingLongitude", 122.0841)
      .set("testField", String.format("test_val_%s", index))
      .build();
    List<StructuredRecord> input = ImmutableList.of(record1, record2);

    deployPipeline(properties, SplunkBatchSource.NAME, "SplunkBatch", schema, input);

    List<Map<String, String>> results = getPipelineResults(index);
    Assert.assertEquals(2, results.size());

    assertRecord(results, record1);
    assertRecord(results, record2);
  }

  @Test
  public void testBatchSinkBatchSize() throws Exception {
    ImmutableMap<String, String> properties = getBasicConfig()
      .put("batchSize", "100")
      .build();

    long index = System.currentTimeMillis();

    Schema schema = Schema.recordOf(
      "output",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ShippingLatitude", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("ShippingLongitude", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("testField", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord record1 = StructuredRecord.builder(schema)
      .set("Name", "testUpdateAccount1")
      .set("NumberOfEmployees", 6)
      .set("ShippingLatitude", 50.4501)
      .set("ShippingLongitude", 30.5234)
      .set("testField", String.format("test_val_%s", index))
      .build();
    StructuredRecord record2 = StructuredRecord.builder(schema)
      .set("Name", "testUpdateAccount2")
      .set("NumberOfEmployees", 1)
      .set("ShippingLatitude", 37.4220)
      .set("ShippingLongitude", 122.0841)
      .set("testField", String.format("test_val_%s", index))
      .build();
    List<StructuredRecord> input = ImmutableList.of(record1, record2);

    deployPipeline(properties, SplunkBatchSource.NAME, "SplunkBatch", schema, input);

    List<Map<String, String>> results = getPipelineResults(index);
    Assert.assertEquals(2, results.size());

    assertRecord(results, record1);
    assertRecord(results, record2);
  }

  private ImmutableMap.Builder<String, String> getBasicConfig() {
    return ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("url", URL_WRITE)
      .put("authenticationType", "token")
      .put("token", TOKEN_HEC)
      .put("connectTimeout", "60000")
      .put("readTimeout", "60000")
      .put("numberOfRetries", "3")
      .put("maxRetryWait", "60000")
      .put("maxRetryJitterWait", "100")
      .put("endpoint", "Event");
  }

  private void assertRecord(List<Map<String, String>> results, StructuredRecord expected) {
    List<Map<String, String>> actual = results.stream()
      .filter(record -> expected.get("Name").equals(record.get("Name")))
      .collect(Collectors.toList());

    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get("NumberOfEmployees").toString(), actual.get(0).get("NumberOfEmployees"));
    Assert.assertEquals(expected.get("ShippingLatitude").toString(), actual.get(0).get("ShippingLatitude"));
    Assert.assertEquals(expected.get("ShippingLongitude").toString(), actual.get(0).get("ShippingLongitude"));
    Assert.assertEquals(expected.get("testField").toString(), actual.get(0).get("testField"));
  }

  private void deployPipeline(Map<String, String> sinkProperties,
                              String pluginName,
                              String applicationPrefix,
                              Schema schema,
                              List<StructuredRecord> input) throws Exception {
    String inputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName, schema));

    ETLStage sink = new ETLStage(pluginName, new ETLPlugin(
      pluginName, BatchSink.PLUGIN_TYPE, sinkProperties, null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app(applicationPrefix + "_" + testName.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  private List<Map<String, String>> getPipelineResults(long index) throws IOException {
    String format = String.format("search index=\"%s\" testField=\"test_val_%s\" | kvform", INDEX, index);
    ImmutableMap<String, String> resultsArguments = ImmutableMap.of("output_mode", "json");
    InputStream inputStream = splunkClient.oneshotSearch(format, resultsArguments);
    List<Map<String, String>> results = new ArrayList<>();
    ResultsReaderJson resultsReaderJson = new ResultsReaderJson(inputStream);
    resultsReaderJson.forEach(results::add);
    return results;
  }
}
