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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.db.pipe.event.common.row.PipeDataTypeTransformer;
import org.apache.iotdb.pipe.api.type.Type;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PipeDataTypeTransformerTest {

  @Test
  public void testTransformToPipeDataType() {
    for (final DataTypeCase dataTypeCase : dataTypeCases()) {
      Assert.assertEquals(
          dataTypeCase.pipeDataType,
          PipeDataTypeTransformer.transformToPipeDataType(dataTypeCase.tsDataType));
    }

    Assert.assertNull(PipeDataTypeTransformer.transformToPipeDataType(null));
  }

  @Test
  public void testUnsupportedDataType() {
    for (final TSDataType unsupportedDataType :
        Arrays.asList(TSDataType.VECTOR, TSDataType.UNKNOWN)) {
      try {
        PipeDataTypeTransformer.transformToPipeDataType(unsupportedDataType);
        Assert.fail("Should reject unsupported TSDataType " + unsupportedDataType);
      } catch (final IllegalArgumentException ignored) {
        // Expected exception
      }
    }
  }

  @Test
  public void testTransformToPipeDataTypeList() {
    Assert.assertNull(PipeDataTypeTransformer.transformToPipeDataTypeList(null));
    Assert.assertEquals(
        Collections.emptyList(),
        PipeDataTypeTransformer.transformToPipeDataTypeList(Collections.emptyList()));

    final List<TSDataType> tsDataTypes = new ArrayList<>();
    final List<Type> pipeDataTypes = new ArrayList<>();
    for (final DataTypeCase dataTypeCase : dataTypeCases()) {
      tsDataTypes.add(dataTypeCase.tsDataType);
      pipeDataTypes.add(dataTypeCase.pipeDataType);
    }
    tsDataTypes.add(null);
    pipeDataTypes.add(null);

    Assert.assertEquals(
        pipeDataTypes, PipeDataTypeTransformer.transformToPipeDataTypeList(tsDataTypes));
  }

  private static List<DataTypeCase> dataTypeCases() {
    return Arrays.asList(
        new DataTypeCase(TSDataType.BOOLEAN, Type.BOOLEAN),
        new DataTypeCase(TSDataType.INT32, Type.INT32),
        new DataTypeCase(TSDataType.INT64, Type.INT64),
        new DataTypeCase(TSDataType.FLOAT, Type.FLOAT),
        new DataTypeCase(TSDataType.DOUBLE, Type.DOUBLE),
        new DataTypeCase(TSDataType.TEXT, Type.TEXT),
        new DataTypeCase(TSDataType.TIMESTAMP, Type.TIMESTAMP),
        new DataTypeCase(TSDataType.DATE, Type.DATE),
        new DataTypeCase(TSDataType.BLOB, Type.BLOB),
        new DataTypeCase(TSDataType.STRING, Type.STRING));
  }

  private static class DataTypeCase {

    private final TSDataType tsDataType;
    private final Type pipeDataType;

    private DataTypeCase(final TSDataType tsDataType, final Type pipeDataType) {
      this.tsDataType = tsDataType;
      this.pipeDataType = pipeDataType;
    }
  }
}
