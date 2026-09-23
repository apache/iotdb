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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.storageengine.load.converter.LoadConvertedInsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipeConvertedInsertTabletStatementTest {

  private boolean enablePartialInsert;

  @Before
  public void setUp() {
    enablePartialInsert = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(enablePartialInsert);
  }

  @Test
  public void testTypeConversionSkipsMissingColumnWithoutException() throws Exception {
    final InsertTabletStatement source = createPartiallyFailedStatementWithMissingColumn();
    final PipeConvertedInsertTabletStatement converted =
        new PipeConvertedInsertTabletStatement(source);

    converted.selfCheckDataTypes(0);
    converted.selfCheckDataTypes(1);

    final Tablet tablet = converted.convertToTablet();
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("s1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(TSDataType.DOUBLE, tablet.getSchemas().get(0).getType());
    Assert.assertArrayEquals(new double[] {1.0, 2.0}, (double[]) tablet.getValues()[0], 0.0);
    Assert.assertNull(converted.getMeasurements()[1]);
    Assert.assertNull(converted.getDataTypes()[1]);
  }

  @Test
  public void testTypeConversionSkipsNullMeasurementWithoutException() throws Exception {
    final InsertTabletStatement source = new InsertTabletStatement();
    source.setDevicePath(new PartialPath("root.sg.d1"));
    source.setMeasurements(new String[] {"s1", "s2"});
    source.setDataTypes(new TSDataType[] {TSDataType.INT32, null});
    source.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.DOUBLE)
        });
    source.setTimes(new long[] {1L, 2L});
    source.setColumns(new Object[] {new int[] {1, 2}, null});
    source.setRowCount(2);
    source.selfCheckDataTypes(1);

    Assert.assertNull(source.getMeasurements()[1]);

    final PipeConvertedInsertTabletStatement converted =
        new PipeConvertedInsertTabletStatement(source);
    converted.selfCheckDataTypes(0);
    converted.selfCheckDataTypes(1);

    final Tablet tablet = converted.convertToTablet();

    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("s1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(TSDataType.DOUBLE, tablet.getSchemas().get(0).getType());
    Assert.assertArrayEquals(new double[] {1.0, 2.0}, (double[]) tablet.getValues()[0], 0.0);
    Assert.assertNull(converted.getMeasurements()[1]);
    Assert.assertNull(converted.getDataTypes()[1]);
  }

  @Test
  public void testLoadTypeConversionSkipsMissingColumnWithoutException() throws Exception {
    final LoadConvertedInsertTabletStatement converted =
        new LoadConvertedInsertTabletStatement(
            createPartiallyFailedStatementWithMissingColumn(), true);

    converted.selfCheckDataTypes(0);
    converted.selfCheckDataTypes(1);

    final Tablet tablet = converted.convertToTablet();
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals(TSDataType.DOUBLE, tablet.getSchemas().get(0).getType());
    Assert.assertArrayEquals(new double[] {1.0, 2.0}, (double[]) tablet.getValues()[0], 0.0);
  }

  private static InsertTabletStatement createPartiallyFailedStatementWithMissingColumn()
      throws Exception {
    final InsertTabletStatement source = new InsertTabletStatement();
    source.setDevicePath(new PartialPath("root.sg.d1"));
    source.setMeasurements(new String[] {"s1", "s2"});
    source.setDataTypes(new TSDataType[] {TSDataType.INT32, TSDataType.INT64});
    source.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.DOUBLE)
        });
    source.setTimes(new long[] {1L, 2L});
    source.setColumns(new Object[] {new int[] {1, 2}});
    source.setRowCount(2);
    source.selfCheckDataTypes(1);
    return source;
  }
}
