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

package io.cdap.plugin.splunk.etl;

import com.google.common.collect.ImmutableSet;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.splunk.sink.batch.SplunkBatchSink;
import io.cdap.plugin.splunk.source.batch.SplunkBatchSource;
import io.cdap.plugin.splunk.source.batch.SplunkMapToRecordTransformer;
import io.cdap.plugin.splunk.source.streaming.SplunkStreamingSource;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tests to verify configuration of Splunk ETL plugins
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
public abstract class BaseSplunkTest extends HydratorTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BaseSplunkTest.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final ArtifactSummary APP_STREAMS = new ArtifactSummary("data-streams", "3.2.0");

  protected static final String TOKEN_HEC = System.getProperty("splunk.test.token.hec");
  protected static final String TOKEN_API = System.getProperty("splunk.test.token.api");
  protected static final String URL_WRITE = System.getProperty("splunk.test.url.write");
  protected static final String URL_READ = System.getProperty("splunk.test.url.read");
  protected static final String INDEX = System.getProperty("splunk.test.index");

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setupBasic() throws Exception {
    assertProperties();

    ArtifactId parentBatch = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());
    ArtifactId parentStreams = NamespaceId.DEFAULT.artifact(APP_STREAMS.getName(), APP_STREAMS.getVersion());

    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), parentBatch.getArtifact(),
                        new ArtifactVersion(parentBatch.getVersion()), true,
                        new ArtifactVersion(parentBatch.getVersion()), true),
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), parentStreams.getArtifact(),
                        new ArtifactVersion(parentStreams.getVersion()), true,
                        new ArtifactVersion(parentStreams.getVersion()), true)
    );

    setupBatchArtifacts(parentBatch, DataPipelineApp.class);
    setupStreamingArtifacts(parentStreams, DataStreamsApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("splunk-plugins", "1.0.0"),
                      parents,
                      SplunkBatchSource.class,
                      SplunkBatchSink.class,
                      SplunkStreamingSource.class,
                      SplunkMapToRecordTransformer.class);
  }

  private static void assertProperties() {
    try {
      Assume.assumeNotNull(TOKEN_HEC, TOKEN_API, URL_WRITE, URL_READ, INDEX);
    } catch (AssumptionViolatedException e) {
      LOG.warn("ETL tests are skipped. Please find the instructions on enabling it at " +
                 "SplunkBatchSourceTest javadoc.");
      throw e;
    }
  }

  protected static Service buildSplunkClient(String context, String token) throws MalformedURLException {
    URL url = new URL(context);
    Map<String, Object> connectionArgs = new HashMap<>();
    connectionArgs.put("host", url.getHost());
    connectionArgs.put("token", token);
    connectionArgs.put("port", url.getPort());
    connectionArgs.put("scheme", url.getProtocol());
    connectionArgs.put("SSLSecurityProtocol", SSLSecurityProtocol.TLSv1_2);
    return Service.connect(connectionArgs);
  }
}
