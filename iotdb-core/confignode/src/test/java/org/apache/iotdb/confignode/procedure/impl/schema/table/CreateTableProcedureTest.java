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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreateTableProcedureTest {
  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final TsTable table = new TsTable("table1");
    table.addColumnSchema(new IdColumnSchema("Id", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("Attr", TSDataType.STRING));
    table.addColumnSchema(
        new MeasurementColumnSchema(
            "Measurement", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
    final CreateTableProcedure createTableProcedure =
        new CreateTableProcedure("root.database1", table);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    createTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(ProcedureType.CREATE_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final CreateTableProcedure deserializedProcedure = new CreateTableProcedure();
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(createTableProcedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(
        createTableProcedure.getTable().getTableName(),
        deserializedProcedure.getTable().getTableName());
    Assert.assertEquals(
        createTableProcedure.getTable().getColumnNum(),
        deserializedProcedure.getTable().getColumnNum());
    Assert.assertEquals(
        createTableProcedure.getTable().getIdNums(), deserializedProcedure.getTable().getIdNums());
  }
}
