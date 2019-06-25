/**
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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class PrimitiveDataListPoolTest {

  @Test
  public void getPrimitiveDataListByDataType() {
  }

  @Test
  public void release() {
    int loop = 100;
    while(loop-- > 0){
      PrimitiveArrayListV2 primitiveArrayList = PrimitiveDataListPool.getInstance().getPrimitiveDataListByDataType(
          TSDataType.INT32);
      testPutAndGet(primitiveArrayList, loop * 100);
      PrimitiveDataListPool.getInstance().release(primitiveArrayList);
      assert PrimitiveDataListPool.getInstance().getPrimitiveDataListSizeByDataType(TSDataType.INT32) == 1;
    }
  }

  public void testPutAndGet(PrimitiveArrayListV2 primitiveArrayList, int count) {
    for (int i = 0; i < count; i++) {
      primitiveArrayList.putTimestamp(i, i);
    }

    assert count == primitiveArrayList.getTotalDataNumber();

    for (int i = 0; i < count; i++) {
      int v = (int) primitiveArrayList.getValue(i);
      Assert.assertEquals((long) i, primitiveArrayList.getTimestamp(i));
      Assert.assertEquals(i, v);
    }
  }
}