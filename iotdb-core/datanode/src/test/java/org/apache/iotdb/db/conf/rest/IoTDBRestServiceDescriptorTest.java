/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.conf.rest;

import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBRestServiceDescriptorTest {

  private IoTDBRestServiceConfig config;
  private long originalMaxRequestBodySizeInBytes;
  private long originalMaxTotalConcurrentRequestBodySizeInBytes;
  private int originalMaxInsertRows;
  private int originalMaxInsertColumns;
  private long originalMaxInsertValues;

  @Before
  public void setUp() {
    config = IoTDBRestServiceDescriptor.getInstance().getConfig();
    originalMaxRequestBodySizeInBytes = config.getRestMaxRequestBodySizeInBytes();
    originalMaxTotalConcurrentRequestBodySizeInBytes =
        config.getRestMaxTotalConcurrentRequestBodySizeInBytes();
    originalMaxInsertRows = config.getRestMaxInsertRows();
    originalMaxInsertColumns = config.getRestMaxInsertColumns();
    originalMaxInsertValues = config.getRestMaxInsertValues();
  }

  @After
  public void tearDown() {
    config.setRestMaxRequestBodySizeInBytes(originalMaxRequestBodySizeInBytes);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(
        originalMaxTotalConcurrentRequestBodySizeInBytes);
    config.setRestMaxInsertRows(originalMaxInsertRows);
    config.setRestMaxInsertColumns(originalMaxInsertColumns);
    config.setRestMaxInsertValues(originalMaxInsertValues);
  }

  @Test
  public void testDefaultRequestBodyMemoryLimit() {
    Assert.assertEquals(
        Runtime.getRuntime().maxMemory() / 20,
        IoTDBRestServiceDescriptor.calculateRequestBodyMemoryLimitInBytes(new TrimProperties()));
  }

  @Test
  public void testRequestBodyMemoryLimitUsesHalfOfFreeMemory() {
    TrimProperties properties = new TrimProperties();
    properties.setProperty("datanode_memory_proportion", "1:1:1:1:1:5");

    Assert.assertEquals(
        Runtime.getRuntime().maxMemory() / 4,
        IoTDBRestServiceDescriptor.calculateRequestBodyMemoryLimitInBytes(properties));
  }

  @Test
  public void testHotReloadRequestLimits() {
    TrimProperties properties = new TrimProperties();
    properties.setProperty("rest_max_request_body_size_in_bytes", "101");
    properties.setProperty("rest_max_total_concurrent_request_body_size_in_bytes", "102");
    properties.setProperty("rest_max_insert_rows", "103");
    properties.setProperty("rest_max_insert_columns", "104");
    properties.setProperty("rest_max_insert_values", "105");

    IoTDBRestServiceDescriptor.getInstance().loadHotModifiedProps(properties);

    Assert.assertEquals(101, config.getRestMaxRequestBodySizeInBytes());
    Assert.assertEquals(102, config.getRestMaxTotalConcurrentRequestBodySizeInBytes());
    Assert.assertEquals(103, config.getRestMaxInsertRows());
    Assert.assertEquals(104, config.getRestMaxInsertColumns());
    Assert.assertEquals(105, config.getRestMaxInsertValues());
  }
}
