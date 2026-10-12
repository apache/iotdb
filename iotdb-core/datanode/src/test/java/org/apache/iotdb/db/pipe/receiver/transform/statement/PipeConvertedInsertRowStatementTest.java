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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeTableStatementDataTypeConvertExecutionVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertNumericRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.utils.NumericRowValues;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

  @Test
  public void testNumericRowDispatchConvertsNumericAndStringFields() throws Exception {
    final InsertNumericRowStatement original = numericRow();
    final long[] originalValues = original.getValues().clone();
    final AtomicInteger executions = new AtomicInteger();
    final TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    final PipeTableStatementDataTypeConvertExecutionVisitor visitor =
        new PipeTableStatementDataTypeConvertExecutionVisitor(
            (statement, database) -> {
              executions.incrementAndGet();
              Assert.assertEquals("db", database);
              Assert.assertTrue(statement instanceof PipeConvertedInsertRowStatement);
              final PipeConvertedInsertRowStatement converted =
                  (PipeConvertedInsertRowStatement) statement;
              Assert.assertTrue(converted.isForceTypeConversion());
              Assert.assertFalse(converted.isNeedInferType());
              Assert.assertTrue(converted.isAligned());
              Assert.assertTrue(converted.isWriteToTable());
              Assert.assertEquals(original.getTime(), converted.getTime());
              Assert.assertEquals(original.getDatabaseName(), converted.getDatabaseName());
              Assert.assertEquals(original.getTableDeviceID(), converted.getTableDeviceID());
              Assert.assertEquals(original.getFirstValueOfIndex(0), converted.getValues()[0]);
              Assert.assertTrue(converted.checkAndCastDataType(1, TSDataType.INT32));
              Assert.assertTrue(converted.checkAndCastDataType(2, TSDataType.STRING));
              Assert.assertTrue(converted.checkAndCastDataType(3, TSDataType.STRING));
              Assert.assertEquals(12, converted.getValues()[1]);
              Assert.assertEquals(
                  new Binary("8.5", TSFileConfig.STRING_CHARSET), converted.getValues()[2]);
              Assert.assertNull(converted.getValues()[3]);
              Assert.assertArrayEquals(
                  new TSDataType[] {
                    TSDataType.STRING, TSDataType.INT32, TSDataType.STRING, TSDataType.STRING
                  },
                  converted.getDataTypes());
              return success;
            });

    Assert.assertEquals(success, visitor.process(original, new Pair<>(success, "db")).orElse(null));
    Assert.assertEquals(1, executions.get());
    Assert.assertArrayEquals(originalValues, original.getValues());
    Assert.assertArrayEquals(
        new TSDataType[] {
          TSDataType.STRING, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT64
        },
        original.getDataTypes());
    Assert.assertTrue(original.getNumericValues().isNull(3));
  }

  @Test
  public void testNumericRowRetryRestoresFailuresOnlyInCopy() throws Exception {
    final InsertNumericRowStatement original = numericRow();
    original.markFailedMeasurement(0, new IllegalArgumentException("tag failure"));
    original.markFailedMeasurement(1, new IllegalArgumentException("field failure"));
    final long[] failedValues = original.getValues().clone();
    final PipeConvertedInsertRowStatement converted = new PipeConvertedInsertRowStatement(original);

    Assert.assertFalse(converted.hasFailedMeasurements());
    Assert.assertEquals(List.of(0), converted.getTagColumnIndices());
    Assert.assertEquals("tag", converted.getMeasurements()[0]);
    Assert.assertEquals("count", converted.getMeasurements()[1]);
    Assert.assertEquals(new Binary("d1", TSFileConfig.STRING_CHARSET), converted.getValues()[0]);
    Assert.assertEquals(12.5, converted.getValues()[1]);
    Assert.assertTrue(converted.checkAndCastDataType(1, TSDataType.INT32));
    Assert.assertEquals(12, converted.getValues()[1]);

    Assert.assertEquals(2, original.getFailedMeasurementNumber());
    Assert.assertNotSame(
        original.getFailedMeasurementInfoMap(), converted.getFailedMeasurementInfoMap());
    Assert.assertNull(original.getMeasurements()[0]);
    Assert.assertNull(original.getMeasurements()[1]);
    Assert.assertNull(original.getDataTypes()[1]);
    Assert.assertTrue(original.getNumericValues().isNull(0));
    Assert.assertTrue(original.getNumericValues().isNull(1));
    Assert.assertArrayEquals(failedValues, original.getValues());
    Assert.assertEquals(12.5, original.getFailedMeasurementInfoMap().get(1).getValue());
  }

  @Test
  public void testNumericRowConversionOwnsMutableMetadataAndPreservesNulls() throws Exception {
    final InsertNumericRowStatement original = numericRow();
    original.getNumericValues().setTag(0, null);
    final PipeConvertedInsertRowStatement converted = new PipeConvertedInsertRowStatement(original);
    Assert.assertNull(converted.getValues()[0]);
    Assert.assertNull(converted.getValues()[3]);
    Assert.assertEquals(original.getTableDeviceID(), converted.getTableDeviceID());
    Assert.assertNotSame(original.getMeasurements(), converted.getMeasurements());
    Assert.assertNotSame(original.getDataTypes(), converted.getDataTypes());
    Assert.assertNotSame(original.getMeasurementSchemas(), converted.getMeasurementSchemas());
    Assert.assertNotSame(original.getColumnCategories(), converted.getColumnCategories());
    converted.swapColumn(1, 2);
    converted.setColumnCategory(TsTableColumnCategory.ATTRIBUTE, 1);
    converted.markFailedMeasurement(2, new IllegalArgumentException("conversion failure"));

    Assert.assertArrayEquals(
        new String[] {"tag", "count", "text", "nullable"}, original.getMeasurements());
    Assert.assertEquals(TsTableColumnCategory.FIELD, original.getColumnCategory(1));
    Assert.assertEquals("count", original.getMeasurementSchemas()[1].getMeasurementName());
    Assert.assertEquals(12.5, original.getFirstValueOfIndex(1));
    Assert.assertEquals(8.5, original.getFirstValueOfIndex(2));
    Assert.assertFalse(original.hasFailedMeasurements());
  }

  private static InsertNumericRowStatement numericRow() throws Exception {
    final InsertNumericRowStatement statement = new InsertNumericRowStatement();
    statement.setDevicePath(new PartialPath("t"));
    statement.setDatabaseName("db");
    statement.setTime(123L);
    statement.setMeasurements(new String[] {"tag", "count", "text", "nullable"});
    statement.setDataTypes(
        new TSDataType[] {
          TSDataType.STRING, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT64
        });
    statement.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD
        });
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("tag", TSDataType.STRING),
          new MeasurementSchema("count", TSDataType.DOUBLE),
          new MeasurementSchema("text", TSDataType.DOUBLE),
          new MeasurementSchema("nullable", TSDataType.INT64)
        });
    final NumericRowValues values = new NumericRowValues(1, 3);
    values.setTag(0, "d1");
    values.setObject(1, TSDataType.DOUBLE, 12.5);
    values.setObject(2, TSDataType.DOUBLE, 8.5);
    values.setNull(3);
    statement.setNumericValues(values);
    return statement;
  }
}
