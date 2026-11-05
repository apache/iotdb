/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PivotConfigurationValidationTest {
  @Test
  public void testRejectMalformedAndOverflowBeforeApply() {
    assertNull(ConfigManager.validatePivotConfiguration(Map.of()));
    assertNull(ConfigManager.validatePivotConfiguration(Map.of("pivot_limit", "0")));
    assertNull(ConfigManager.validatePivotConfiguration(Map.of("pivot_input_max_rows", "-1")));
    assertNull(
        ConfigManager.validatePivotConfiguration(
            Map.of(
                "pivot_limit",
                "2147483647",
                "pivot_input_buffer_size_in_bytes",
                "9223372036854775807")));
    for (Map.Entry<String, String> entry :
        Map.of(
                "pivot_limit", "2147483648",
                "pivot_input_max_rows", "not-a-number",
                "pivot_input_buffer_size_in_bytes", "9223372036854775808")
            .entrySet()) {
      var status =
          ConfigManager.validatePivotConfiguration(Map.of(entry.getKey(), entry.getValue()));
      assertNotNull(status);
      assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    }
  }
}
