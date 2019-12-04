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
import io.cdap.plugin.splunk.source.SplunkSourceConfigBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SearchHelper}
 */
public class SearchHelperTest {

  @Test
  public void testDecorateSearchStringNull() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setSearchString(null)
        .build();

    String actual = SearchHelper.decorateSearchString(config);

    Assert.assertNull(actual);
  }

  @Test
  public void testDecorateSearchStringEmpty() {
    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setSearchString("")
        .build();

    String actual = SearchHelper.decorateSearchString(config);

    Assert.assertNotNull(actual);
    Assert.assertTrue(actual.isEmpty());
  }

  @Test
  public void testDecorateSearchStringDecorate() {
    String expected = "search * | kvform";

    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setSearchString("search *")
        .build();

    String actual = SearchHelper.decorateSearchString(config);

    Assert.assertNotNull(actual);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testDecorateSearchString() {
    String expected = "search *|kvform";

    SplunkSourceConfig config =
      new SplunkSourceConfigBuilder(SplunkSourceConfigBuilder.CONFIG)
        .setSearchString(expected)
        .build();

    String actual = SearchHelper.decorateSearchString(config);

    Assert.assertNotNull(actual);
    Assert.assertEquals(actual, expected);
  }
}
