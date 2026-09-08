/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class QueryThreadPoolConfigTest {

  @Test
  public void testDefaultsAndPositiveSizes() throws Exception {
    IoTDBConfig config = new IoTDBConfig();
    assertSizes(config, 20, 10, 4);
    assertEquals(
        "20",
        ConfigurationFileUtils.getConfigurationDefaultValue("coordinator_read_executor_size"));
    assertEquals(
        "10",
        ConfigurationFileUtils.getConfigurationDefaultValue("coordinator_scheduled_executor_size"));
    assertEquals(
        "4",
        ConfigurationFileUtils.getConfigurationDefaultValue(
            "fragment_instance_notification_thread_count"));

    for (int invalid : new int[] {0, -1}) {
      assertThrows(
          IllegalArgumentException.class, () -> config.setCoordinatorReadExecutorSize(invalid));
      assertThrows(
          IllegalArgumentException.class,
          () -> config.setCoordinatorScheduledExecutorSize(invalid));
      assertThrows(
          IllegalArgumentException.class,
          () -> config.setFragmentInstanceNotificationThreadCount(invalid));
    }
    assertSizes(config, 20, 10, 4);
  }

  @Test
  public void testStartupOverridesAreRestartOnly() throws Exception {
    IoTDBDescriptor descriptor = new IoTDBDescriptor();
    TrimProperties properties = new TrimProperties();
    properties.setProperty("coordinator_read_executor_size", "3");
    properties.setProperty("coordinator_scheduled_executor_size", "2");
    properties.setProperty("fragment_instance_notification_thread_count", "1");
    descriptor.loadProperties(properties);
    assertSizes(descriptor.getConfig(), 3, 2, 1);

    properties.setProperty("coordinator_read_executor_size", "6");
    properties.setProperty("coordinator_scheduled_executor_size", "5");
    properties.setProperty("fragment_instance_notification_thread_count", "4");
    descriptor.loadHotModifiedProps(properties);
    assertSizes(descriptor.getConfig(), 3, 2, 1);
  }

  private static void assertSizes(IoTDBConfig config, int read, int scheduled, int notification) {
    assertEquals(read, config.getCoordinatorReadExecutorSize());
    assertEquals(scheduled, config.getCoordinatorScheduledExecutorSize());
    assertEquals(notification, config.getFragmentInstanceNotificationThreadCount());
  }
}
