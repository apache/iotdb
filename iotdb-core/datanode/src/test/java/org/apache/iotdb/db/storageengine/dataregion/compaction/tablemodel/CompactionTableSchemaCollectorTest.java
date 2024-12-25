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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionTableSchemaNotMatchException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchema;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CompactionTableSchemaCollectorTest {
  @Test
  public void test1() {
    CompactionTableSchema compactionTableSchema = new CompactionTableSchema("t1");

    List<IMeasurementSchema> measurementSchemaList1 = new ArrayList<>();
    measurementSchemaList1.add(new MeasurementSchema("id1", TSDataType.STRING));
    measurementSchemaList1.add(new MeasurementSchema("id2", TSDataType.STRING));
    measurementSchemaList1.add(new MeasurementSchema("id3", TSDataType.STRING));
    measurementSchemaList1.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemaList1.add(new MeasurementSchema("s2", TSDataType.INT32));
    List<ColumnCategory> columnTypeList1 = new ArrayList<>();
    columnTypeList1.add(ColumnCategory.ID);
    columnTypeList1.add(ColumnCategory.ID);
    columnTypeList1.add(ColumnCategory.ID);
    columnTypeList1.add(ColumnCategory.MEASUREMENT);
    columnTypeList1.add(ColumnCategory.MEASUREMENT);
    TableSchema tableSchema1 = new TableSchema("t1", measurementSchemaList1, columnTypeList1);
    compactionTableSchema.merge(tableSchema1);
    Assert.assertEquals(3, compactionTableSchema.getColumnSchemas().size());

    List<IMeasurementSchema> measurementSchemaList2 = new ArrayList<>();
    measurementSchemaList2.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemaList2.add(new MeasurementSchema("id1", TSDataType.STRING));
    measurementSchemaList2.add(new MeasurementSchema("id2", TSDataType.STRING));
    measurementSchemaList2.add(new MeasurementSchema("id3", TSDataType.STRING));
    measurementSchemaList2.add(new MeasurementSchema("s2", TSDataType.INT32));
    List<ColumnCategory> columnTypeList2 = new ArrayList<>();
    columnTypeList2.add(ColumnCategory.MEASUREMENT);
    columnTypeList2.add(ColumnCategory.ID);
    columnTypeList2.add(ColumnCategory.ID);
    columnTypeList2.add(ColumnCategory.ID);
    columnTypeList2.add(ColumnCategory.MEASUREMENT);
    TableSchema tableSchema2 = new TableSchema("t1", measurementSchemaList2, columnTypeList2);
    compactionTableSchema.merge(tableSchema2);
    Assert.assertEquals(3, compactionTableSchema.getColumnSchemas().size());

    List<IMeasurementSchema> measurementSchemaList3 = new ArrayList<>();
    measurementSchemaList3.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemaList3.add(new MeasurementSchema("id1", TSDataType.STRING));
    measurementSchemaList3.add(new MeasurementSchema("id2", TSDataType.STRING));
    measurementSchemaList3.add(new MeasurementSchema("id3", TSDataType.STRING));
    measurementSchemaList3.add(new MeasurementSchema("s2", TSDataType.INT32));
    measurementSchemaList3.add(new MeasurementSchema("id4", TSDataType.STRING));
    List<ColumnCategory> columnTypeList3 = new ArrayList<>();
    columnTypeList3.add(ColumnCategory.MEASUREMENT);
    columnTypeList3.add(ColumnCategory.ID);
    columnTypeList3.add(ColumnCategory.ID);
    columnTypeList3.add(ColumnCategory.ID);
    columnTypeList3.add(ColumnCategory.MEASUREMENT);
    columnTypeList3.add(ColumnCategory.ID);
    TableSchema tableSchema3 = new TableSchema("t1", measurementSchemaList3, columnTypeList3);
    compactionTableSchema.merge(tableSchema3);

    Assert.assertEquals(4, compactionTableSchema.getColumnSchemas().size());

    List<IMeasurementSchema> measurementSchemaList4 = new ArrayList<>();
    measurementSchemaList4.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemaList4.add(new MeasurementSchema("id1", TSDataType.STRING));
    measurementSchemaList4.add(new MeasurementSchema("id2", TSDataType.STRING));
    measurementSchemaList4.add(new MeasurementSchema("id4", TSDataType.STRING));
    measurementSchemaList4.add(new MeasurementSchema("s2", TSDataType.INT32));
    measurementSchemaList4.add(new MeasurementSchema("id3", TSDataType.STRING));
    List<ColumnCategory> columnTypeList4 = new ArrayList<>();
    columnTypeList4.add(ColumnCategory.MEASUREMENT);
    columnTypeList4.add(ColumnCategory.ID);
    columnTypeList4.add(ColumnCategory.ID);
    columnTypeList4.add(ColumnCategory.ID);
    columnTypeList4.add(ColumnCategory.MEASUREMENT);
    columnTypeList4.add(ColumnCategory.ID);
    TableSchema tableSchema4 = new TableSchema("t1", measurementSchemaList4, columnTypeList4);
    try {
      compactionTableSchema.merge(tableSchema4);
    } catch (CompactionTableSchemaNotMatchException e) {
      return;
    }
    Assert.fail();
  }
}
