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

import org.apache.iotdb.commons.snapshot.SnapshotStreamFactory;

import org.junit.Assert;
import org.junit.Test;

public class ConfigNodeConfigTest {

  @Test
  public void testSnapshotBufferSizeMaxDefault() {
    final ConfigNodeConfig configNodeConfig = new ConfigNodeConfig();
    // The code default must stay aligned with SnapshotStreamFactory's default cap.
    Assert.assertEquals(
        SnapshotStreamFactory.DEFAULT_BUFFER_SIZE_MAX,
        configNodeConfig.getConfigNodeSnapshotBufferSizeMax());

    configNodeConfig.setConfigNodeSnapshotBufferSizeMax(256 * 1024L);
    Assert.assertEquals(256 * 1024L, configNodeConfig.getConfigNodeSnapshotBufferSizeMax());
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> configNodeConfig.setConfigNodeSnapshotBufferSizeMax((long) Integer.MAX_VALUE + 1));
    Assert.assertEquals(256 * 1024L, configNodeConfig.getConfigNodeSnapshotBufferSizeMax());
  }
}
