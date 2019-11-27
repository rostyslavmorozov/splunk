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

package io.cdap.plugin.splunk;

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
public class ValidationAssertions {

  public static void assertValidationFailed(MockFailureCollector failureCollector, List<String> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    String attribute = CauseAttributes.STAGE_CONFIG;
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, attribute);
    Assert.assertEquals(paramNames.size(), causeList.size());
    IntStream.range(0, paramNames.size())
      .forEachOrdered(i -> Assert.assertEquals(
        paramNames.get(i), causeList.get(i).getAttribute(attribute)));
  }

  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String attribute) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(attribute) != null)
      .collect(Collectors.toList());
  }
}
