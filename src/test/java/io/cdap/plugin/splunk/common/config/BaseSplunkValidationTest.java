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

package io.cdap.plugin.splunk.common.config;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class to simplify config validation.
 */
public abstract class BaseSplunkValidationTest {

  private static final String MOCK_STAGE = "mockStage";

  protected static void assertValidationSucceed(BaseSplunkConfig config) {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  protected static void assertValidationFailed(BaseSplunkConfig config, List<String> paramNames) {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);

    assertValidationFailed(failureCollector, paramNames);
  }

  protected static void assertValidationConnectionFailed(BaseSplunkConfig config,
                                                         List<String> paramNames) {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateConnection(failureCollector);

    assertValidationFailed(failureCollector, paramNames);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<String> paramNames) {
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = failureCollector.getValidationFailures().get(0).getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(paramNames.size(), causeList.size());
    IntStream.range(0, paramNames.size())
      .forEachOrdered(i -> Assert.assertEquals(
        paramNames.get(i), causeList.get(i).getAttribute(CauseAttributes.STAGE_CONFIG)));
  }
}
