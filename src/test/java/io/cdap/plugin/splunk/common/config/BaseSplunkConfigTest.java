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

import org.junit.Test;

import java.util.Collections;

/**
 * Tests for {@link BaseSplunkConfig}
 */
public class BaseSplunkConfigTest extends BaseSplunkValidationTest {

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

    assertValidationSucceed(config);
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

    assertValidationFailed(config, Collections.singletonList(BaseSplunkConfig.PROPERTY_AUTHENTICATION_TYPE));
  }
}
