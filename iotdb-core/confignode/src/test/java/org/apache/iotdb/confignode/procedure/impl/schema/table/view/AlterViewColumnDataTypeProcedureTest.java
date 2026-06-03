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

package org.apache.iotdb.confignode.procedure.impl.schema.table.view;

import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AlterViewColumnDataTypeProcedureTest {
  @Test
  public void serializeDeserializeTest() throws IOException {
    assertSerializeDeserialize(
        new AlterViewColumnDataTypeProcedure(
            "database1", "table1", "0", "columnName", TSDataType.INT64, false),
        ProcedureType.ALTER_VIEW_COLUMN_DATATYPE_PROCEDURE,
        new AlterViewColumnDataTypeProcedure(false));
    assertSerializeDeserialize(
        new AlterViewColumnDataTypeProcedure(
            "database1", "table1", "0", "columnName", TSDataType.INT64, true),
        ProcedureType.PIPE_ENRICHED_ALTER_VIEW_COLUMN_DATATYPE_PROCEDURE,
        new AlterViewColumnDataTypeProcedure(true));
  }

  private void assertSerializeDeserialize(
      final AlterViewColumnDataTypeProcedure procedure,
      final ProcedureType procedureType,
      final AlterViewColumnDataTypeProcedure deserializedProcedure)
      throws IOException {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    procedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(procedureType.getTypeCode(), byteBuffer.getShort());

    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(procedure, deserializedProcedure);
  }
}
