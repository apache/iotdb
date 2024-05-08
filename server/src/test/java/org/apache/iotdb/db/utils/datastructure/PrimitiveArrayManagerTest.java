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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;

import org.junit.Assert;
import org.junit.Test;

public class PrimitiveArrayManagerTest {
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Test
  public void testGetArrayRowCount() {

    Assert.assertEquals(
        1224827,
        PrimitiveArrayManager.getArrayRowCount(1224826 * config.getPrimitiveArraySize() + 1));

    Assert.assertEquals(
        1224826, PrimitiveArrayManager.getArrayRowCount(1224826 * config.getPrimitiveArraySize()));

    Assert.assertEquals(1, PrimitiveArrayManager.getArrayRowCount(config.getPrimitiveArraySize()));

    Assert.assertEquals(
        1, PrimitiveArrayManager.getArrayRowCount(config.getPrimitiveArraySize() - 1));

    Assert.assertEquals(
        2, PrimitiveArrayManager.getArrayRowCount(config.getPrimitiveArraySize() + 1));
  }
}
