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
package org.apache.iotdb.db.writelog;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class WriteLogNodeTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean enableWal;

  @Before
  public void setUp() {
    enableWal = config.isEnableWal();
    config.setEnableWal(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setEnableWal(enableWal);
  }

  @Test
  public void testWriteLogAndSync() throws IOException, IllegalPathException {
    // this test uses a dummy insert log node to insert a few logs and flushes them
    // then reads the logs from file
    String identifier = "root.logTestDevice";

    ByteBuffer[] byteBuffers = new ByteBuffer[2];
    byteBuffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);
    logNode.initBuffer(byteBuffers);

    InsertRowPlan bwInsertPlan =
        new InsertRowPlan(
            new PartialPath(identifier),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            new String[] {"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath(identifier + ".s1"));

    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    Object[] columns = new Object[4];
    columns[0] = new double[4];
    columns[1] = new long[4];
    columns[2] = new Binary[4];
    columns[3] = new boolean[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((long[]) columns[1])[r] = 1;
      ((Binary[]) columns[2])[r] = new Binary("hh" + r);
      ((boolean[]) columns[3])[r] = false;
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath(identifier), new String[] {"s1", "s2", "s3", "s4"}, dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    tabletPlan.setStart(0);
    tabletPlan.setEnd(4);

    tabletPlan.markFailedMeasurementInsertion(1, new Exception());

    logNode.write(bwInsertPlan);
    logNode.write(deletePlan);
    logNode.write(tabletPlan);

    logNode.close();

    File walFile =
        new File(config.getWalDir() + File.separator + identifier + File.separator + "wal1");
    assertTrue(walFile.exists());

    ILogReader reader = logNode.getLogReader();
    assertEquals(bwInsertPlan, reader.next());
    assertEquals(deletePlan, reader.next());
    InsertTabletPlan newPlan = (InsertTabletPlan) reader.next();
    assertEquals(newPlan.getMeasurements().length, 3);
    reader.close();

    ByteBuffer[] array = logNode.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }

  @Test
  public void testNotifyFlush() throws IOException, IllegalPathException {
    // this test writes a few logs and sync them
    // then calls notifyStartFlush() and notifyEndFlush() to delete old file
    String identifier = "root.logTestDevice";

    ByteBuffer[] byteBuffers = new ByteBuffer[2];
    byteBuffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);
    logNode.initBuffer(byteBuffers);

    InsertRowPlan bwInsertPlan =
        new InsertRowPlan(
            new PartialPath(identifier),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            new String[] {"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath(identifier + ".s1"));

    logNode.write(bwInsertPlan);
    logNode.notifyStartFlush();
    logNode.write(deletePlan);
    logNode.notifyStartFlush();

    ILogReader logReader = logNode.getLogReader();
    assertEquals(bwInsertPlan, logReader.next());
    assertEquals(deletePlan, logReader.next());
    logReader.close();

    logNode.notifyEndFlush();
    logReader = logNode.getLogReader();
    assertEquals(deletePlan, logReader.next());
    logReader.close();

    logNode.notifyEndFlush();
    logReader = logNode.getLogReader();
    assertFalse(logReader.hasNext());
    logReader.close();

    ByteBuffer[] array = logNode.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }

  @Test
  public void testSyncThreshold() throws IOException, IllegalPathException {
    // this test checks that if more logs than threshold are written, a sync will be triggered.
    int flushWalThreshold = config.getFlushWalThreshold();
    config.setFlushWalThreshold(2);

    ByteBuffer[] byteBuffers = new ByteBuffer[2];
    byteBuffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");
    logNode.initBuffer(byteBuffers);

    InsertRowPlan bwInsertPlan =
        new InsertRowPlan(
            new PartialPath("root.logTestDevice"),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            new String[] {"1.0", "15", "str", "false"});
    DeletePlan deletePlan =
        new DeletePlan(Long.MIN_VALUE, 50, new PartialPath("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan);

    File walFile =
        new File(
            config.getWalDir() + File.separator + "root.logTestDevice" + File.separator + "wal1");
    assertFalse(walFile.exists());

    logNode.write(deletePlan);
    System.out.println("Waiting for wal file to be created");
    while (!walFile.exists()) {}

    assertTrue(walFile.exists());

    ByteBuffer[] array = logNode.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
    config.setFlushWalThreshold(flushWalThreshold);
  }

  @Test
  public void testDelete() throws IOException, IllegalPathException {
    // this test uses a dummy insert log node to insert a few logs and flushes them
    // then deletes the node

    ByteBuffer[] byteBuffers = new ByteBuffer[2];
    byteBuffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");
    logNode.initBuffer(byteBuffers);

    InsertRowPlan bwInsertPlan =
        new InsertRowPlan(
            new PartialPath("logTestDevice"),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            new String[] {"1.0", "15", "str", "false"});
    DeletePlan deletePlan =
        new DeletePlan(Long.MIN_VALUE, 50, new PartialPath("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan);
    logNode.write(deletePlan);

    logNode.forceSync();

    File walFile =
        new File(
            config.getWalDir() + File.separator + "root.logTestDevice" + File.separator + "wal1");
    System.out.println("Waiting for wal to be created");
    while (!walFile.exists()) {}

    assertTrue(new File(logNode.getLogDirectory()).exists());
    ByteBuffer[] array = logNode.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
    assertFalse(new File(logNode.getLogDirectory()).exists());
  }

  @Test
  public void testOverSizedWAL() throws IOException, IllegalPathException {
    // this test uses a dummy insert log node to insert an over-sized log and assert exception
    // caught
    ByteBuffer[] byteBuffers = new ByteBuffer[2];
    byteBuffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice.oversize");
    logNode.initBuffer(byteBuffers);

    int neededSize = 0;
    InsertRowPlan bwInsertPlan =
        new InsertRowPlan(
            new PartialPath("root.logTestDevice.oversize"),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            new String[] {"1.0", "15", new String(new char[65 * 1024 * 1024]), "false"});

    boolean caught = false;
    try {
      logNode.write(bwInsertPlan);
    } catch (IOException e) {
      caught = true;
      Pattern r = Pattern.compile("\\d+");
      Matcher m = r.matcher(e.getMessage());
      if (m.find()) {
        neededSize = Integer.valueOf(m.group());
      }
    }
    assertTrue(caught);

    // if last insertplan failed by overflow,it can not take affect the next insertplan
    InsertRowPlan bwInsertPlan2 =
        new InsertRowPlan(
            new PartialPath("root.logTestDevice.oversize"),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            // try to apply a insertplan whose size will fill the entire logBufferWorking
            new String[] {
              "1.0",
              "15",
              new String(
                  new char
                      [(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2) - 109]),
              "false"
            });
    caught = false;
    try {
      logNode.write(bwInsertPlan2);
    } catch (IOException e) {
      caught = true;
    }
    assertFalse(caught);

    ByteBuffer[] array = logNode.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }

    // try to set wal_buffer_size according to error message
    IoTDBDescriptor.getInstance().getConfig().setWalBufferSize(neededSize);
    WriteLogNode logNode2 = new ExclusiveWriteLogNode("root.logTestDevice.oversize");
    logNode2.initBuffer(byteBuffers);
    ByteBuffer[] byteBuffers2 = new ByteBuffer[2];
    byteBuffers2[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers2[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    logNode2.initBuffer(byteBuffers2);
    caught = false;
    try {
      logNode2.write(bwInsertPlan);
    } catch (IOException e) {
      caught = true;
    }
    assertFalse(caught);
    array = logNode2.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }

    // try to set wal_buffer_size less than error message
    IoTDBDescriptor.getInstance().getConfig().setWalBufferSize(neededSize - 1);
    WriteLogNode logNode3 = new ExclusiveWriteLogNode("root.logTestDevice.oversize");
    logNode3.initBuffer(byteBuffers);
    ByteBuffer[] byteBuffers3 = new ByteBuffer[2];
    byteBuffers3[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    byteBuffers3[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    logNode3.initBuffer(byteBuffers3);
    caught = false;
    try {
      logNode3.write(bwInsertPlan);
    } catch (IOException e) {
      caught = true;
    }
    assertTrue(caught);
    array = logNode3.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }

  @Test
  public void testBufferOverflowAndRewrite() throws IOException, IllegalPathException {
    String identifier = "root.logTestDevice";

    InsertRowPlan insertPlan =
        new InsertRowPlan(
            new PartialPath(identifier),
            100,
            new String[] {"s1", "s2", "s3", "s4"},
            new TSDataType[] {
              TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN
            },
            new String[] {"1.0", "15", "str", "false"});

    // get InsertRowPlan byte size
    ByteBuffer tmpBuffer =
        ByteBuffer.allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    insertPlan.serialize(tmpBuffer);
    int size = tmpBuffer.position();
    // allocate buffers
    ByteBuffer[] byteBuffers = new ByteBuffer[2];
    byteBuffers[0] = ByteBuffer.allocateDirect(size + 1);
    byteBuffers[1] = ByteBuffer.allocateDirect(size + 1);
    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);
    logNode.initBuffer(byteBuffers);
    // write InsertRowPlan to WAL buffer
    logNode.write(insertPlan);
    insertPlan.setTime(200);
    logNode.write(insertPlan);

    logNode.close();

    File walFile =
        new File(config.getWalDir() + File.separator + identifier + File.separator + "wal1");
    assertTrue(walFile.exists());

    ILogReader reader = logNode.getLogReader();
    insertPlan.setTime(100);
    assertEquals(insertPlan, reader.next());
    insertPlan.setTime(200);
    assertEquals(insertPlan, reader.next());
    reader.close();

    ByteBuffer[] array = logNode.delete();
    for (ByteBuffer byteBuffer : array) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }
}
