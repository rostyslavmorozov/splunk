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
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link BaseSplunkConfig}
 */
public class BaseSplunkConfigTest {

  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void testValidate() {
    BaseSplunkConfig config = new BaseSplunkConfig(
      "reference",
      "basic",
      "apiToken",
      "userName",
      60000,
      60000,
      3,
      60000,
      100);

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidAuthenticationType() {
    BaseSplunkConfig config = new BaseSplunkConfig(
      "reference",
      "invalid",
      "apiToken",
      "userName",
      60000,
      60000,
      3,
      60000,
      100);

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(1, causeList.size());
    Assert.assertEquals(BaseSplunkConfig.PROPERTY_AUTHENTICATION_TYPE, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
