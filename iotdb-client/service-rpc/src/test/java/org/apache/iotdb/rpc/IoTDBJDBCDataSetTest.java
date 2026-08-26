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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rpc;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class IoTDBJDBCDataSetTest {

  @Test
  public void testInitializeValueBuffersUsesDataTypeInterfaces() {
    byte[][] buffers =
        IoTDBJDBCDataSet.initializeValueBuffers(
            Arrays.asList(
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
                TSDataType.DATE,
                TSDataType.TIMESTAMP,
                TSDataType.TEXT,
                TSDataType.STRING,
                TSDataType.BLOB,
                TSDataType.OBJECT));

    Assert.assertEquals(Byte.BYTES, buffers[0].length);
    Assert.assertEquals(Integer.BYTES, buffers[1].length);
    Assert.assertEquals(Long.BYTES, buffers[2].length);
    Assert.assertEquals(Float.BYTES, buffers[3].length);
    Assert.assertEquals(Double.BYTES, buffers[4].length);
    Assert.assertEquals(Integer.BYTES, buffers[5].length);
    Assert.assertEquals(Long.BYTES, buffers[6].length);
    Assert.assertNull(buffers[7]);
    Assert.assertNull(buffers[8]);
    Assert.assertNull(buffers[9]);
    Assert.assertNull(buffers[10]);
  }

  @Test
  public void testInitializeValueBuffersRejectsInternalTypes() {
    assertUnsupported(TSDataType.UNKNOWN);
    assertUnsupported(TSDataType.VECTOR);
  }

  @Test
  public void testJdbcValueReadersUseTypeServices() {
    Assert.assertEquals(
        "42",
        TypeServices.JDBC_STRING_READER_SERVICE
            .call(Type.fromTsDataType(TSDataType.INT32))
            .apply(BytesUtils.intToBytes(42)));
    Assert.assertEquals(
        42,
        TypeServices.JDBC_OBJECT_READER_SERVICE
            .call(Type.fromTsDataType(TSDataType.INT32))
            .apply(BytesUtils.intToBytes(42)));

    byte[] binary = new byte[] {1, 2, 3};
    Assert.assertEquals(
        new Binary(binary),
        TypeServices.JDBC_OBJECT_READER_SERVICE
            .call(Type.fromTsDataType(TSDataType.BLOB))
            .apply(binary));

    Assert.assertNull(
        TypeServices.JDBC_STRING_READER_SERVICE.call(UnknownType.UNKNOWN).apply(new byte[0]));
    Assert.assertNull(
        TypeServices.JDBC_OBJECT_READER_SERVICE.call(UnknownType.UNKNOWN).apply(new byte[0]));
  }

  private static void assertUnsupported(TSDataType dataType) {
    UnSupportedDataTypeException exception =
        Assert.assertThrows(
            UnSupportedDataTypeException.class,
            () -> IoTDBJDBCDataSet.initializeValueBuffers(Arrays.asList(dataType)));
    Assert.assertTrue(
        exception
            .getMessage()
            .endsWith(String.format(IoTDBJDBCDataSet.DATA_TYPE_NOT_SUPPORTED, dataType)));
  }
}
