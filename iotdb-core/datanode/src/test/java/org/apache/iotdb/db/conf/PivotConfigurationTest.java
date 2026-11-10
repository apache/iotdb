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

package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PivotConfigurationTest {
  @Test
  public void testStartupAndReloadRetainEffectiveValues() throws Exception {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    int oldLimit = config.getPivotLimit();
    long oldBytes = config.getPivotInputBufferSizeInBytes();
    int oldRows = config.getPivotInputMaxRows();
    IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    try {
      assertEquals("100000", ConfigurationFileUtils.getConfigurationDefaultValue("pivot_limit"));
      assertEquals(
          "67108864",
          ConfigurationFileUtils.getConfigurationDefaultValue("pivot_input_buffer_size_in_bytes"));
      assertEquals(
          "1000000", ConfigurationFileUtils.getConfigurationDefaultValue("pivot_input_max_rows"));

      TrimProperties properties = new TrimProperties();
      properties.setProperty("pivot_limit", "11");
      properties.setProperty("pivot_input_buffer_size_in_bytes", "1024");
      properties.setProperty("pivot_input_max_rows", "9");
      descriptor.loadProperties(properties);
      assertEquals(11, config.getPivotLimit());
      assertEquals(1024, config.getPivotInputBufferSizeInBytes());
      assertEquals(9, config.getPivotInputMaxRows());

      properties.setProperty("pivot_limit", "12");
      properties.setProperty("pivot_input_buffer_size_in_bytes", "2048");
      properties.setProperty("pivot_input_max_rows", "10");
      descriptor.loadHotModifiedProps(properties);
      assertEquals(12, config.getPivotLimit());
      assertEquals(2048, config.getPivotInputBufferSizeInBytes());
      assertEquals(10, config.getPivotInputMaxRows());

      properties.setProperty("pivot_limit", "2147483648");
      properties.setProperty("pivot_input_buffer_size_in_bytes", "9223372036854775808");
      properties.setProperty("pivot_input_max_rows", "not-a-number");
      descriptor.loadHotModifiedProps(properties);
      assertEquals(12, config.getPivotLimit());
      assertEquals(2048, config.getPivotInputBufferSizeInBytes());
      assertEquals(10, config.getPivotInputMaxRows());
      assertEquals("12", ConfigurationFileUtils.getAppliedProperties().get("pivot_limit"));
      assertEquals(
          "2048",
          ConfigurationFileUtils.getAppliedProperties().get("pivot_input_buffer_size_in_bytes"));
      assertEquals("10", ConfigurationFileUtils.getAppliedProperties().get("pivot_input_max_rows"));

      properties.setProperty("pivot_limit", "0");
      properties.setProperty("pivot_input_buffer_size_in_bytes", "-1");
      properties.setProperty("pivot_input_max_rows", "0");
      descriptor.loadHotModifiedProps(properties);
      assertEquals(12, config.getPivotLimit());
      assertEquals(2048, config.getPivotInputBufferSizeInBytes());
      assertEquals(10, config.getPivotInputMaxRows());
    } finally {
      config.setPivotLimit(oldLimit);
      config.setPivotInputBufferSizeInBytes(oldBytes);
      config.setPivotInputMaxRows(oldRows);
    }
  }
}
