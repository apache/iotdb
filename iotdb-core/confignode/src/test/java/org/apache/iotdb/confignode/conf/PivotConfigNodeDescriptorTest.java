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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PivotConfigNodeDescriptorTest {
  @Test
  public void testReloadAndShowEffectiveValues() throws Exception {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    int oldLimit = config.getPivotLimit();
    long oldBytes = config.getPivotInputBufferSizeInBytes();
    int oldRows = config.getPivotInputMaxRows();
    try {
      TrimProperties properties = new TrimProperties();
      properties.setProperty("pivot_limit", "3");
      properties.setProperty("pivot_input_buffer_size_in_bytes", "100");
      properties.setProperty("pivot_input_max_rows", "4");
      ConfigNodeDescriptor.getInstance().loadHotModifiedProps(properties);
      assertEquals(3, config.getPivotLimit());
      assertEquals(100, config.getPivotInputBufferSizeInBytes());
      assertEquals(4, config.getPivotInputMaxRows());

      properties.setProperty("pivot_limit", "0");
      properties.setProperty("pivot_input_buffer_size_in_bytes", "-1");
      properties.setProperty("pivot_input_max_rows", "2147483648");
      ConfigNodeDescriptor.getInstance().loadHotModifiedProps(properties);
      assertEquals("3", ConfigurationFileUtils.getAppliedProperties().get("pivot_limit"));
      assertEquals(
          "100",
          ConfigurationFileUtils.getAppliedProperties().get("pivot_input_buffer_size_in_bytes"));
      assertEquals("4", ConfigurationFileUtils.getAppliedProperties().get("pivot_input_max_rows"));
    } finally {
      config.setPivotLimit(oldLimit);
      config.setPivotInputBufferSizeInBytes(oldBytes);
      config.setPivotInputMaxRows(oldRows);
    }
  }
}
