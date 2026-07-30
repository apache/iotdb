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

import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.TSDataTypeTestUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public class PrimitiveArrayManagerTest {
  private DataNodeMemoryConfig dataNodeMemoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();

  @Test
  public void testGetArrayRowCount() {

    Assert.assertEquals(1224827, PrimitiveArrayManager.getArrayRowCount(1224826 * ARRAY_SIZE + 1));

    Assert.assertEquals(1224826, PrimitiveArrayManager.getArrayRowCount(1224826 * ARRAY_SIZE));

    Assert.assertEquals(1, PrimitiveArrayManager.getArrayRowCount(ARRAY_SIZE));

    Assert.assertEquals(1, PrimitiveArrayManager.getArrayRowCount(ARRAY_SIZE - 1));

    Assert.assertEquals(2, PrimitiveArrayManager.getArrayRowCount(ARRAY_SIZE + 1));
  }

  @Test
  public void testUpdateLimits() {
    double AMPLIFICATION_FACTOR = 1.5;

    /** threshold total size of arrays for all data types */
    double POOLED_ARRAYS_MEMORY_THRESHOLD =
        dataNodeMemoryConfig.getBufferedArraysMemoryManager().getTotalMemorySizeInBytes()
            / AMPLIFICATION_FACTOR;
    // LIMITS should be updated if (TOTAL_ALLOCATION_REQUEST_COUNT.get() > limitUpdateThreshold)
    int totalDataTypeSize = 0;
    for (TSDataType dataType : TSDataTypeTestUtils.getSupportedTypes()) {
      totalDataTypeSize += dataType.getDataTypeSize();
    }

    double limit = POOLED_ARRAYS_MEMORY_THRESHOLD / ARRAY_SIZE / totalDataTypeSize;
    for (int i = 0; i < limit + 1; i++) {
      for (TSDataType type : TSDataTypeTestUtils.getSupportedTypes()) {
        Object o = PrimitiveArrayManager.allocate(type);
        switch (type) {
          case BOOLEAN:
            Assert.assertTrue(o instanceof boolean[]);
            Assert.assertEquals(ARRAY_SIZE, ((boolean[]) o).length);
            break;
          case INT32:
          case DATE:
            Assert.assertTrue(o instanceof int[]);
            Assert.assertEquals(ARRAY_SIZE, ((int[]) o).length);
            break;
          case INT64:
          case TIMESTAMP:
            Assert.assertTrue(o instanceof long[]);
            Assert.assertEquals(ARRAY_SIZE, ((long[]) o).length);
            break;
          case FLOAT:
            Assert.assertTrue(o instanceof float[]);
            Assert.assertEquals(ARRAY_SIZE, ((float[]) o).length);
            break;
          case DOUBLE:
            Assert.assertTrue(o instanceof double[]);
            Assert.assertEquals(ARRAY_SIZE, ((double[]) o).length);
            break;
          case TEXT:
          case BLOB:
          case OBJECT:
          case STRING:
            Assert.assertTrue(o instanceof Binary[]);
            Assert.assertEquals(ARRAY_SIZE, ((Binary[]) o).length);
            break;
        }
      }
    }
  }
}
