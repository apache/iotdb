/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.receiver.transform.statement;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;

public class PipeConvertedInsertRowStatementTest {

  @Test
  public void testTransferTypeKeepsNullValue() throws Exception {
    final InsertRowStatement statement = new InsertRowStatement();
    statement.setDevicePath(new PartialPath("root.sg.d1"));
    statement.setMeasurements(new String[] {"s0"});
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s0", TSDataType.INT32)});
    statement.setDataTypes(new TSDataType[] {null});
    statement.setTime(1L);
    statement.setValues(new Object[] {null});
    statement.setNeedInferType(true);

    final PipeConvertedInsertRowStatement convertedStatement =
        new PipeConvertedInsertRowStatement(statement);
    convertedStatement.transferType(ZoneId.systemDefault());

    Assert.assertEquals(TSDataType.INT32, convertedStatement.getDataTypes()[0]);
    Assert.assertEquals("s0", convertedStatement.getMeasurements()[0]);
    Assert.assertNull(convertedStatement.getValues()[0]);
    Assert.assertFalse(convertedStatement.isNeedInferType());
  }
}
