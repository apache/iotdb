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
import org.apache.iotdb.db.sync.externalpipe.operation.DeleteOperation;
import org.apache.iotdb.db.sync.externalpipe.operation.InsertOperation;
import org.apache.iotdb.db.sync.externalpipe.operation.Operation;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PipeOpManagerTest {
  public static final String TMP_DIR = "target" + File.separator + "PipeOpManagerTest";
  private static final String seqTsFileName1 = TMP_DIR + File.separator + "test1.tsfile";
  private static final String seqModsFileName1 = seqTsFileName1 + ".mods";
  private static final String unSeqTsFileName1 = TMP_DIR + File.separator + "test2.unseq.tsfile";
  private static final String unSeqModsFileName1 = unSeqTsFileName1 + ".mods";
  public static final String DEFAULT_TEMPLATE = "template";
  public static final List<String> delFileList = new LinkedList<>();

  private static final String bigSeqTsFileName1 = TMP_DIR + File.separator + "test1.big.seq.tsfile";
  private static final String bigSeqTsFileName2 = TMP_DIR + File.separator + "test2.big.seq.tsfile";
  private static final String bigSeqTsFileName3 = TMP_DIR + File.separator + "test3.big.seq.tsfile";

  private static int oldMaxNumberOfPointsInPage;

  @BeforeClass
  public static void prepareTestData() throws Exception {
    oldMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

    EnvironmentUtils.envSetUp();

    createSeqTsfile(seqTsFileName1);
    delFileList.add(seqTsFileName1);
    creatSeqModsFile(seqModsFileName1);
    delFileList.add(seqModsFileName1);

    createUnSeqTsfile(unSeqTsFileName1);
    delFileList.add(unSeqTsFileName1);
    creatUnSeqModsFile(unSeqModsFileName1);
    delFileList.add(unSeqModsFileName1);

    createBigSeqTsfile(bigSeqTsFileName1, 1, -1);
    delFileList.add(bigSeqTsFileName1);
    createBigSeqTsfile(bigSeqTsFileName2, 2, 100);
    delFileList.add(bigSeqTsFileName2);
    createBigSeqTsfile(bigSeqTsFileName3, 3, 57);
    delFileList.add(bigSeqTsFileName3);
  }

  @AfterClass
  public static void removeTestData() throws Exception {
    for (String fileName : delFileList) {
      File file = new File(fileName);
      if (file.exists()) {
        file.delete();
      }
    }

    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanAllDir();

    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(oldMaxNumberOfPointsInPage);
  }

  private static void createSeqTsfile(String tsfilePath) throws Exception {
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

    tsRecord = new TSRecord(1617206403004L, "root.lemming.device3");
    dPoint1 = new FloatDataPoint("sensor1", 4.1f);
    dPoint2 = new IntDataPoint("sensor2", 42);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  private static void createUnSeqTsfile(String tsfilePath) throws Exception {
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
    TSRecord tsRecord = new TSRecord(1617206403001L, "root2.lemming.device1");
    DataPoint dPoint1 = new FloatDataPoint("sensor1", 1.1f);
    DataPoint dPoint2 = new IntDataPoint("sensor2", 12);
    DataPoint dPoint3 = new IntDataPoint("sensor3", 13);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403002L, "root2.lemming.device2");
    dPoint2 = new IntDataPoint("sensor2", 22);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403003L, "root2.lemming.device3");
    dPoint1 = new FloatDataPoint("sensor1", 33.1f);
    dPoint2 = new IntDataPoint("sensor2", 332);
    dPoint3 = new IntDataPoint("sensor3", 333);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    tsRecord = new TSRecord(1617206403004L, "root2.lemming.device3");
    dPoint1 = new FloatDataPoint("sensor1", 44.1f);
    dPoint2 = new IntDataPoint("sensor2", 442);
    dPoint3 = new IntDataPoint("sensor3", 443);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);
    tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

    // close TsFile
    tsFileWriter.close();
  }

  private static void creatSeqModsFile(String modsFilePath) throws IllegalPathException {
    Modification[] modifications =
        new Modification[] {
          new Deletion(new PartialPath("root.lemming.device2.sensor2"), 2, 1617206403002L),
          new Deletion(
              new PartialPath("root.lemming.device3.sensor1"), 3, 1617206403003L, 1617206403009L),
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

  private static void creatUnSeqModsFile(String modsFilePath) throws IllegalPathException {
    Modification[] modifications =
        new Modification[] {
          new Deletion(new PartialPath("root2.lemming.device1.sensor1"), 2, 1617206403001L),
          new Deletion(new PartialPath("root2.lemming.device2.*"), 3, 2, Long.MAX_VALUE),
          new Deletion(
              new PartialPath("root1.lemming.**"), 3, 2, Long.MAX_VALUE), // useless entry for root1
        };

    try (ModificationFile mFile = new ModificationFile(modsFilePath)) {
      for (Modification mod : modifications) {
        mFile.write(mod);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
    }
  }

  @Test(timeout = 10_000L)
  public void testOpManager() throws IOException {
    PipeOpManager pipeOpManager = new PipeOpManager(null);

    String sgName1 = "root1";
    String sgName2 = "root2";

    TsFileOpBlock tsFileOpBlock1 = new TsFileOpBlock(sgName1, seqTsFileName1, 1);
    pipeOpManager.appendOpBlock(sgName1, tsFileOpBlock1);
    TsFileOpBlock tsFileOpBlock2 = new TsFileOpBlock(sgName2, unSeqTsFileName1, 2);
    pipeOpManager.appendOpBlock(sgName2, tsFileOpBlock2);

    long count1 = tsFileOpBlock1.getDataCount();
    assertEquals(8, count1);
    for (int i = 0; i < count1; i++) {
      Operation operation = pipeOpManager.getOperation(sgName1, i, 8);
      System.out.println("=== data" + i + ": " + operation + ", "); //
      assertEquals("root1", operation.getStorageGroup());
    }

    Operation operation = pipeOpManager.getOperation(sgName1, 0, 18);
    InsertOperation insertOperation = (InsertOperation) operation;
    System.out.println("+++ data10" + ": " + operation + ", ");
    assertEquals(
        "root.lemming.device1.sensor1", insertOperation.getDataList().get(0).left.toString());

    pipeOpManager.commitData(sgName1, count1 - 1);
    operation = pipeOpManager.getOperation(sgName1, 9, 18);
    System.out.println("+++ data11" + ": " + operation + ", ");
    assertNull(operation);

    operation = pipeOpManager.getOperation(sgName2, 6, 18);
    System.out.println("+++ data12" + ": " + operation + ", ");
    assertEquals(4, operation.getDataCount());

    insertOperation = (InsertOperation) operation;
    assertEquals(
        "root2.lemming.device3.sensor3", insertOperation.getDataList().get(0).left.toString());
    assertEquals(1617206403003L, insertOperation.getDataList().get(0).right.get(0).getTimestamp());
    assertEquals("333", insertOperation.getDataList().get(0).right.get(0).getValue().toString());
  }

  @Test(timeout = 10_000L)
  public void testOpManager_Mods() throws IOException {
    PipeOpManager pipeOpManager = new PipeOpManager(null);

    String sgName1 = "root1";
    // String sgName2 = "root2";

    TsFileOpBlock tsFileOpBlock1 = new TsFileOpBlock(sgName1, seqTsFileName1, seqModsFileName1, 1);
    pipeOpManager.appendOpBlock(sgName1, tsFileOpBlock1);
    TsFileOpBlock tsFileOpBlock2 =
        new TsFileOpBlock(sgName1, unSeqTsFileName1, unSeqModsFileName1, 2);
    pipeOpManager.appendOpBlock(sgName1, tsFileOpBlock2);

    long count1 = tsFileOpBlock1.getDataCount();
    assertEquals(8, count1);
    for (int i = 0; i < 18; i++) {
      Operation operation = pipeOpManager.getOperation(sgName1, i, 8);
      assertEquals(sgName1, operation.getStorageGroup());
    }

    // == test batch data in TsFile1 + .mods
    Operation operation = pipeOpManager.getOperation(sgName1, 0, 18);
    assertEquals(8, operation.getDataCount());

    InsertOperation insertOperation = (InsertOperation) operation;
    int i = 0;
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("1.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 1;
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("12", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 2;
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("13", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 3;
    assertEquals(1, insertOperation.getDataList().get(i).right.size());
    assertNull(insertOperation.getDataList().get(i).right.get(0));

    i = 4;
    assertEquals(1, insertOperation.getDataList().get(i).right.size());
    assertNull(insertOperation.getDataList().get(i).right.get(0));

    i = 5;
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("32", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 6;
    assertEquals(1, insertOperation.getDataList().get(i).right.size());
    assertNull(insertOperation.getDataList().get(i).right.get(0));

    i = 7;
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("42", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    // == test batch data in TsFile2 + mods
    operation = pipeOpManager.getOperation(sgName1, 8, 18);
    assertEquals(10, operation.getDataCount());

    insertOperation = (InsertOperation) operation;
    i = 0;
    assertEquals(
        "root2.lemming.device1.sensor1", insertOperation.getDataList().get(i).left.toString());
    assertEquals(1, insertOperation.getDataList().get(i).right.size());
    assertNull(insertOperation.getDataList().get(i).right.get(0));

    i = 1;
    assertEquals(
        "root2.lemming.device1.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("12", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 2;
    assertEquals(
        "root2.lemming.device1.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403001L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("13", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 3;
    assertEquals(
        "root2.lemming.device2.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1, insertOperation.getDataList().get(i).right.size());
    assertNull(insertOperation.getDataList().get(i).right.get(0));

    i = 4;
    assertEquals(
        "root2.lemming.device3.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("33.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 5;
    assertEquals(
        "root2.lemming.device3.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("332", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 6;
    assertEquals(
        "root2.lemming.device3.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403003L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("333", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 7;
    assertEquals(
        "root2.lemming.device3.sensor1", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("44.1", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 8;
    assertEquals(
        "root2.lemming.device3.sensor2", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("442", insertOperation.getDataList().get(i).right.get(0).getValue().toString());

    i = 9;
    assertEquals(
        "root2.lemming.device3.sensor3", insertOperation.getDataList().get(i).left.getFullPath());
    assertEquals(1617206403004L, insertOperation.getDataList().get(i).right.get(0).getTimestamp());
    assertEquals("443", insertOperation.getDataList().get(i).right.get(0).getValue().toString());
  }

  @Test(timeout = 10_000L)
  public void testOpManager_deletion() throws IOException, IllegalPathException {
    PipeOpManager pipeOpManager = new PipeOpManager(null);

    String sgName1 = "root1";
    String sgName2 = "root2";

    TsFileOpBlock tsFileOpBlock1 = new TsFileOpBlock(sgName1, seqTsFileName1, seqModsFileName1, 1);
    pipeOpManager.appendOpBlock(sgName1, tsFileOpBlock1);
    TsFileOpBlock tsFileOpBlock2 =
        new TsFileOpBlock(sgName1, unSeqTsFileName1, unSeqModsFileName1, 2);
    pipeOpManager.appendOpBlock(sgName2, tsFileOpBlock2);

    pipeOpManager.commitData(sgName1, tsFileOpBlock1.getDataCount() - 1);
    pipeOpManager.commitData(sgName2, tsFileOpBlock2.getDataCount() - 1);
    assertTrue(pipeOpManager.isEmpty());

    PartialPath partialPath = new PartialPath("root.a.**");
    DeletionOpBlock deletionOpBlock = new DeletionOpBlock("root.a", partialPath, -100, 200, 5);

    // == test pipeOpManager.appendOpBlock etc.
    pipeOpManager.appendOpBlock(sgName1, deletionOpBlock);

    long beginIndex = pipeOpManager.getFirstAvailableIndex(sgName1);
    assertEquals(8, beginIndex);

    Operation operation = pipeOpManager.getOperation(sgName1, beginIndex, 10);
    assertEquals(beginIndex, operation.getStartIndex());
    assertEquals(1, operation.getDataCount());

    DeleteOperation deleteOperation = (DeleteOperation) operation;
    assertNotNull(deleteOperation);

    assertEquals(partialPath, deleteOperation.getDeletePath());
    assertEquals(-100, deleteOperation.getStartTime());
    assertEquals(200, deleteOperation.getEndTime());

    // == test pipeOpManager.appendDeletionOpBlock etc.
    String sgName = "root.a";
    Deletion deletion = new Deletion(partialPath, 0, -200, 400);
    pipeOpManager.appendDeletionOpBlock(sgName, deletion, 4);

    beginIndex = pipeOpManager.getFirstAvailableIndex(sgName);
    assertEquals(0, beginIndex);

    operation = pipeOpManager.getOperation(sgName, beginIndex, 10);
    assertEquals(beginIndex, operation.getStartIndex());
    assertEquals(1, operation.getDataCount());

    deleteOperation = (DeleteOperation) operation;
    assertNotNull(deleteOperation);

    assertEquals(partialPath, deleteOperation.getDeletePath());
    assertEquals(-200, deleteOperation.getStartTime());
    assertEquals(400, deleteOperation.getEndTime());
  }

  // == test big seq files and 1 chunk contains multiple pages.
  private static void createBigSeqTsfile(String tsfilePath, int seed, int maxPointNumInPage)
      throws Exception {
    File file = new File(tsfilePath);
    if (file.exists()) {
      file.delete();
    }

    if (maxPointNumInPage > 0) {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(maxPointNumInPage);
    }

    Schema schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor2", TSDataType.INT32, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor3", TSDataType.INT64, TSEncoding.RLE));

    TsFileWriter tsFileWriter = new TsFileWriter(file, schema);

    long ts = 1617206403000L;
    TSRecord tsRecord;
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < 1000; j++) {
        int k = seed * 100000000 + i * 1000 + j;
        ts++;
        tsRecord = new TSRecord(ts, "root.lemming.device1");
        tsRecord.addTuple(new FloatDataPoint("sensor1", k * 1.3f));
        tsRecord.addTuple(new IntDataPoint("sensor2", k));
        tsRecord.addTuple(new LongDataPoint("sensor3", k * k));
        tsFileWriter.write(tsRecord);
      }
      tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

      for (int j = 0; j < 1000; j++) {
        int k = seed * 100000000 + i * 2000 + j;
        ts++;
        tsRecord = new TSRecord(ts, "root.lemming.device2");
        tsRecord.addTuple(new IntDataPoint("sensor2", k));
        tsFileWriter.write(tsRecord);
      }
      tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once

      for (int j = 0; j < 1000; j++) {
        int k = seed * 100000000 + i * 3000 + j;
        ts++;
        tsRecord = new TSRecord(ts, "root.lemming.device3");
        tsRecord.addTuple(new FloatDataPoint("sensor1", k * 1.3f));
        tsRecord.addTuple(new IntDataPoint("sensor2", k));
        tsFileWriter.write(tsRecord);
      }
      tsFileWriter.flushAllChunkGroups(); // flush above data to disk at once
    }
    // close TsFile
    tsFileWriter.close();
  }

  @Test
  public void testManyBigTsfiles() throws IOException {
    PipeOpManager pipeOpManager = new PipeOpManager(null);

    String sgName1 = "root.test1";
    pipeOpManager.appendOpBlock(sgName1, new TsFileOpBlock(sgName1, bigSeqTsFileName1, 1));
    pipeOpManager.appendOpBlock(sgName1, new TsFileOpBlock(sgName1, bigSeqTsFileName2, 2));
    pipeOpManager.appendOpBlock(sgName1, new TsFileOpBlock(sgName1, bigSeqTsFileName3, 3));

    long idx = pipeOpManager.getFirstAvailableIndex(sgName1);
    long sum = 0;
    Operation operation;
    int bulkSize = 1000;
    while (true) {
      operation = pipeOpManager.getOperation(sgName1, idx, bulkSize);
      if (operation == null) {
        System.out.println("operation == null, idx=" + idx + " length=" + bulkSize);
        assertEquals(18000000, idx);
        break;
      }
      long count = operation.getDataCount();
      // System.out.println("idx=" + idx + ", " + "count=" + count);
      if (count == 0) {
        break;
      }
      idx += count;
      sum += count;
      pipeOpManager.commitData(sgName1, idx - 1);
    }
    // System.out.println("data sum = " + sum);
    assertEquals(18000000, sum);

    bulkSize = 777;
    while (true) {
      operation = pipeOpManager.getOperation(sgName1, idx, bulkSize);
      if (operation == null) {
        System.out.println("operation == null, idx=" + idx + " length=" + bulkSize);
        assertEquals(18000000, idx);
        break;
      }
      long count = operation.getDataCount();
      // System.out.println("idx=" + idx + ", " + "count=" + count);
      if (count == 0) {
        break;
      }
      idx += count;
      sum += count;
      pipeOpManager.commitData(sgName1, idx - 1);
    }
    // System.out.println("data sum = " + sum);
    assertEquals(18000000, sum);
  }
}
