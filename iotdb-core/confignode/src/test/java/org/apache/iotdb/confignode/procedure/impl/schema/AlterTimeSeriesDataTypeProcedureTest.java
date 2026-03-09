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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.tree.AlterTimeSeriesOperationType;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AlterTimeSeriesDataTypeProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    String queryId = "1";
    MeasurementPath measurementPath = new MeasurementPath("root.sg1.d1.s1");
    AlterTimeSeriesDataTypeProcedure alterTimeSeriesDataTypeProcedure =
        new AlterTimeSeriesDataTypeProcedure(
            queryId,
            measurementPath,
            AlterTimeSeriesOperationType.ALTER_DATA_TYPE.getTypeValue(),
            TSDataType.FLOAT,
            false);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    alterTimeSeriesDataTypeProcedure.serialize(dataOutputStream);

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.ALTER_TIMESERIES_DATATYPE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    AlterTimeSeriesDataTypeProcedure deserializedProcedure =
        new AlterTimeSeriesDataTypeProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(queryId, deserializedProcedure.getQueryId());
    Assert.assertEquals("root.sg1.d1.s1", deserializedProcedure.getmeasurementPath().getFullPath());
  }
}
