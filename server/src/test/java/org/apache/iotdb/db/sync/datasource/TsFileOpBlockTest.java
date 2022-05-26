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

package org.apache.iotdb.db.sync.datasource;

import org.apache.iotdb.db.sync.externalpipe.operation.InsertOperation;
import org.apache.iotdb.db.sync.externalpipe.operation.Operation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TsFileOpBlockTest {

  public static final String TMP_DIR = "target";
  private static final String tsFileName1 = TMP_DIR + File.separator + "test1.tsfile";
  public static final String DEFAULT_TEMPLATE = "template";

  @Before
  public void prepareTestData() throws Exception {
    createTsfile(tsFileName1);
  }

  @After
  public void removeTestData() throws Exception {
    File file = new File(tsFileName1);
    if (file.exists()) {
      file.delete();
    }
  }

  private void createTsfile(String tsfilePath) throws Exception {
    File file = new File(tsfilePath);
    if (file.exists()) {
      file.delete();
    }

    Schema schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(file, schema);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1617206403001L, "root.lemming.device1");
    DataPoint dPoint1 = new FloatDataPoint("sensor1", 1.1f);
    DataPoint dPoint2 = new IntDataPoint("sensor2", 12);
    DataPoint dPoint3 = new IntDataPoint("sensor3", 13);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403002L, "root.lemming.device2");
    dPoint2 = new IntDataPoint("sensor2", 22);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403003L, "root.lemming.device3");
    dPoint1 = new FloatDataPoint("sensor1", 3.1f);
    dPoint2 = new IntDataPoint("sensor2", 32);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  @Test(timeout = 10_000L)
  public void testSingleReadEntry() throws IOException {
    TsFileOpBlock tsFileOpBlock = new TsFileOpBlock("root", tsFileName1, 0);

    assertEquals("root", tsFileOpBlock.getStorageGroup());
    assertEquals(0, tsFileOpBlock.getBeginIndex());
    assertEquals(6, tsFileOpBlock.getDataCount());
    assertEquals(6, tsFileOpBlock.getNextIndex());

    tsFileOpBlock.setBeginIndex(2);
    assertEquals(8, tsFileOpBlock.getNextIndex());

    Operation operation = null;
    for (int i = 0; i < tsFileOpBlock.getDataCount(); i++) {
      operation = tsFileOpBlock.getOperation(i + 2, 1);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(1, operation.getDataCount());
      assertEquals(i + 2, operation.getStartIndex());
      assertEquals(i + 3, operation.getEndIndex());

      assertEquals(true, operation instanceof InsertOperation);
      InsertOperation insertOperation = (InsertOperation) operation;
      assertEquals(1, insertOperation.getDataList().size());
      // System.out.println("=== data" + i + ": " + operation + ((InsertOperation)
      // operation).getDataList());
    }

    InsertOperation insertOperation = (InsertOperation) operation;
    assertEquals(
        "root.lemming.device3.sensor2", insertOperation.getDataList().get(0).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(0).right.get(0).getTimestamp());
    assertEquals("32", insertOperation.getDataList().get(0).right.get(0).getValue().toString());

    for (int i = 0; i <= tsFileOpBlock.getDataCount() - 3; i++) {
      operation = tsFileOpBlock.getOperation(i + 2, 3);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(3, operation.getDataCount());
      assertEquals(i + 2, operation.getStartIndex());
      assertEquals(i + 5, operation.getEndIndex());
      // System.out.println("=== data" + i + ": " + operation);
    }

    for (long i = 6; i < 8; i++) {
      operation = tsFileOpBlock.getOperation(i, 3);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(8 - i, operation.getDataCount());
      assertEquals(i, operation.getStartIndex());
      assertEquals(8, operation.getEndIndex());
      // System.out.println("=== data" + i + ": " + operation);
    }

    tsFileOpBlock.close();
  }
}
