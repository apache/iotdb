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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
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
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TsFileOpBlockTest {

  public final String TMP_DIR = "target";
  private final String tsFileName1 = TMP_DIR + File.separator + "test1.tsfile";
  private final String tsFileName2 = TMP_DIR + File.separator + "test2.tsfile";
  private final String modsFileName2 = tsFileName2 + ".mods";
  private final String tsFileName3 = TMP_DIR + File.separator + "test3.tsfile";
  private final String modsFileName3 = tsFileName3 + ".mods";
  public final List<String> fileNameList = new LinkedList<>();

  public final String DEFAULT_TEMPLATE = "template";

  @Before
  public void prepareTestData() throws Exception {
    createTsfile1(tsFileName1);
    fileNameList.add(tsFileName1);

    createTsfile2(tsFileName2);
    fileNameList.add(tsFileName2);
    creatModsFile2(modsFileName2);
    fileNameList.add(modsFileName2);

    createTsfile2(tsFileName3);
    fileNameList.add(tsFileName3);
    creatModsFile3(modsFileName3);
    fileNameList.add(modsFileName3);
  }

  @After
  public void removeTestData() throws Exception {
    for (String fileName : fileNameList) {
      File file = new File(fileName);
      if (file.exists()) {
        file.delete();
      }
    }
  }

  private void createTsfile1(String tsfilePath) throws Exception {
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
  public void testOpBlock() throws IOException {
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

    int k = 0;
    assertEquals(
        "root.lemming.device3.sensor2", insertOperation.getDataList().get(k).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(k).right.get(0).getTimestamp());
    assertEquals("32", insertOperation.getDataList().get(k).right.get(0).getValue().toString());

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

  // == test TsFile + .mods

  private void createTsfile2(String tsfilePath) throws Exception {
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

    tsRecord = new TSRecord(1617206403004L, "root.lemming.device1");
    dPoint1 = new FloatDataPoint("sensor1", 4.1f);
    dPoint2 = new IntDataPoint("sensor2", 42);
    dPoint3 = new IntDataPoint("sensor3", 43);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  private void creatModsFile2(String modsFilePath) throws IllegalPathException {
    Modification[] modifications =
        new Modification[] {
          // new Deletion(new PartialPath(new String[] {"d1", "s2"}), 1, 2),
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 2, 1),
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 3, 2, 5),
          new Deletion(new PartialPath("root.lemming.**"), 11, 1, Long.MAX_VALUE)
        };

    try (ModificationFile mFile = new ModificationFile(modsFilePath)) {
      for (Modification mod : modifications) {
        mFile.write(mod);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {;
    }
  }

  private void creatModsFile3(String modsFilePath) throws IllegalPathException {
    Modification[] modifications =
        new Modification[] {
          new Deletion(new PartialPath("root.lemming.device1.sensor1"), 2, 1617206403001L),
          new Deletion(new PartialPath("root.lemming.device2.*"), 3, 2, Long.MAX_VALUE),
        };

    try (ModificationFile mFile = new ModificationFile(modsFilePath)) {
      for (Modification mod : modifications) {
        mFile.write(mod);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {;
    }
  }

  @Test(timeout = 10_000L)
  public void testOpBlockMods2() throws IOException {

    List<Modification> modificationList = null;
    try (ModificationFile mFile = new ModificationFile(modsFileName2)) {
      modificationList = (List<Modification>) mFile.getModifications();
    }
    // System.out.println("=== data: " + modificationList);

    TsFileOpBlock tsFileOpBlock = new TsFileOpBlock("root", tsFileName2, modsFileName2, 0);

    assertEquals("root", tsFileOpBlock.getStorageGroup());
    assertEquals(0, tsFileOpBlock.getBeginIndex());
    assertEquals(9, tsFileOpBlock.getDataCount());
    assertEquals(9, tsFileOpBlock.getNextIndex());

    // == check setBeginIndex()
    tsFileOpBlock.setBeginIndex(55);
    assertEquals(64, tsFileOpBlock.getNextIndex());

    // == check result before and after calling tsFileOpBlock.getOperation()
    assertNull(tsFileOpBlock.getFullPathToDeletionMap());
    assertNull(tsFileOpBlock.getModificationList());
    Operation operation = tsFileOpBlock.getOperation(55, 1);
    ;
    assertNotNull(tsFileOpBlock.getFullPathToDeletionMap());
    assertEquals(modificationList, tsFileOpBlock.getModificationList());
    assertEquals(9, tsFileOpBlock.getDataCount());

    // == check tsFileOpBlock.getOperation()
    for (int i = 0; i < tsFileOpBlock.getDataCount(); i++) {
      operation = tsFileOpBlock.getOperation(i + 55, 1);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(1, operation.getDataCount());
      assertEquals(i + 55, operation.getStartIndex());
      assertEquals(i + 56, operation.getEndIndex());

      assertEquals(true, operation instanceof InsertOperation);
      InsertOperation insertOperation = (InsertOperation) operation;
      assertEquals(1, insertOperation.getDataList().size());
      // System.out.println("=== data" + i + ": " + operation + ((InsertOperation)
      // operation).getDataList());
    }

    // == check deleted data caused by .mods file
    operation = tsFileOpBlock.getOperation(55, 15);
    assertEquals(9, operation.getDataCount());
    InsertOperation insertOperation = (InsertOperation) operation;

    int i = 0;
    assertEquals(
        "root.lemming.device1.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 1;
    assertEquals(
        "root.lemming.device1.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 2;
    assertEquals(
        "root.lemming.device1.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 3;
    assertEquals(
        "root.lemming.device2.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 4;
    assertEquals(
        "root.lemming.device3.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 5;
    assertEquals(
        "root.lemming.device3.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    // assertEquals(1617206403003L,
    // insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    // assertEquals("32", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    tsFileOpBlock.close();
  }

  @Test(timeout = 10_000L)
  public void testOpBlockMods3() throws IOException {

    List<Modification> modificationList = null;
    try (ModificationFile mFile = new ModificationFile(modsFileName3)) {
      modificationList = (List<Modification>) mFile.getModifications();
    }

    TsFileOpBlock tsFileOpBlock = new TsFileOpBlock("root", tsFileName2, modsFileName3, 0);

    assertEquals("root", tsFileOpBlock.getStorageGroup());
    assertEquals(0, tsFileOpBlock.getBeginIndex());
    assertEquals(9, tsFileOpBlock.getDataCount());
    assertEquals(9, tsFileOpBlock.getNextIndex());

    // == check setBeginIndex()
    tsFileOpBlock.setBeginIndex(55);
    assertEquals(64, tsFileOpBlock.getNextIndex());

    // == check result before and after calling tsFileOpBlock.getOperation()
    assertNull(tsFileOpBlock.getFullPathToDeletionMap());
    assertNull(tsFileOpBlock.getModificationList());
    Operation operation = tsFileOpBlock.getOperation(55, 1);

    assertNotNull(tsFileOpBlock.getFullPathToDeletionMap());
    assertEquals(modificationList, tsFileOpBlock.getModificationList());
    assertEquals(9, tsFileOpBlock.getDataCount());

    // == check tsFileOpBlock.getOperation()
    for (int i = 0; i < tsFileOpBlock.getDataCount(); i++) {
      operation = tsFileOpBlock.getOperation(i + 55, 1);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(1, operation.getDataCount());
      assertEquals(i + 55, operation.getStartIndex());
      assertEquals(i + 56, operation.getEndIndex());

      assertEquals(true, operation instanceof InsertOperation);
      InsertOperation insertOperation = (InsertOperation) operation;
      assertEquals(1, insertOperation.getDataList().size());
      // System.out.println("=== data" + i + ": " + operation + ((InsertOperation)
      // operation).getDataList());
    }

    // == check deleted data caused by .mods file
    operation = tsFileOpBlock.getOperation(55, 20);
    assertEquals(9, operation.getDataCount());
    InsertOperation insertOperation = (InsertOperation) operation;

    int i = 0;
    assertEquals(
        "root.lemming.device1.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 1;
    assertEquals(
        "root.lemming.device1.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("12", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 2;
    assertEquals(
        "root.lemming.device1.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("13", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 3;
    assertEquals(
        "root.lemming.device2.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(null, insertOperation.getDataList().get(i).right.get(0));

    i = 4;
    assertEquals(
        "root.lemming.device3.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("3.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 5;
    assertEquals(
        "root.lemming.device3.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("32", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 6;
    assertEquals(
        "root.lemming.device1.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("4.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 7;
    assertEquals(
        "root.lemming.device1.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("42", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 8;
    assertEquals(
        "root.lemming.device1.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("43", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    // == test getting old data and page cache
    operation = tsFileOpBlock.getOperation(59, 20);
    assertEquals(5, operation.getDataCount());
    insertOperation = (InsertOperation) operation;

    i = 0;
    assertEquals(
        "root.lemming.device3.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("3.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 1;
    assertEquals(
        "root.lemming.device3.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("32", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 2;
    assertEquals(
        "root.lemming.device1.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("4.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 3;
    assertEquals(
        "root.lemming.device1.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("42", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 4;
    assertEquals(
        "root.lemming.device1.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("43", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    tsFileOpBlock.close();
  }
}
