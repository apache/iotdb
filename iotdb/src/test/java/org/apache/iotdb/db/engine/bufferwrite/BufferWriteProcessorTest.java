/**
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

package org.apache.iotdb.db.engine.bufferwrite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.PathUtils;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileSchemaUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferWriteProcessorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessorTest.class);

  Action bfflushaction = new Action() {

    @Override
    public void act() throws ActionException {

    }
  };

  Action bfcloseaction = new Action() {

    @Override
    public void act() throws ActionException {
    }
  };

  Action fnflushaction = new Action() {

    @Override
    public void act() throws ActionException {

    }
  };

  private int groupSizeInByte;
  private TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
  private Map<String, Action> parameters = new HashMap<>();
  private BufferWriteProcessor bufferwrite;
  private Directories directories = Directories.getInstance();
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;

  private String insertPath = "insertPath";

  @Before
  public void setUp() throws Exception {
    parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
    parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
    parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);
    // origin value
    groupSizeInByte = TsFileConf.groupSizeInByte;
    // new value
    TsFileConf.groupSizeInByte = 1024;
    // init metadata
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    // recovery value
    TsFileConf.groupSizeInByte = groupSizeInByte;
    // clean environment
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(insertPath);
  }

  @Test
  public void testWriteAndAbnormalRecover()
      throws WriteProcessException, InterruptedException, IOException, ProcessorException {
    bufferwrite = new BufferWriteProcessor(directories.getFolderForTest(), deviceId, insertPath,
        parameters, SysTimeVersionController.INSTANCE,
        FileSchemaUtils.constructFileSchema(deviceId));
    for (int i = 1; i < 100; i++) {
      bufferwrite.write(deviceId, measurementId, i, dataType, String.valueOf(i));
    }
    // waiting for the end of flush
    TimeUnit.SECONDS.sleep(2);
    File dataFile = PathUtils.getBufferWriteDir(deviceId);
    // check file
    String restoreFilePath = insertPath + ".restore";
    File restoreFile = new File(dataFile, restoreFilePath);
    assertTrue(restoreFile.exists());
    File insertFile = new File(dataFile, insertPath);
    long insertFileLength = insertFile.length();
    FileOutputStream fileOutputStream = new FileOutputStream(insertFile.getPath(), true);
    fileOutputStream.write(new byte[20]);
    fileOutputStream.close();
    assertEquals(insertFileLength + 20, insertFile.length());
    // copy restore file
    File file = new File("temp");
    restoreFile.renameTo(file);
    bufferwrite.close();
    file.renameTo(restoreFile);
    BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(
        directories.getFolderForTest(), deviceId,
        insertPath, parameters, SysTimeVersionController.INSTANCE,
        FileSchemaUtils.constructFileSchema(deviceId));
    assertTrue(insertFile.exists());
    assertEquals(insertFileLength, insertFile.length());
    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = bufferWriteProcessor
        .queryBufferWriteData(deviceId, measurementId, dataType, Collections.emptyMap());
    assertTrue(pair.left.isEmpty());
    assertEquals(1, pair.right.size());
    ChunkMetaData chunkMetaData = pair.right.get(0);
    assertEquals(measurementId, chunkMetaData.getMeasurementUid());
    assertEquals(dataType, chunkMetaData.getTsDataType());
    bufferWriteProcessor.close();
    assertFalse(restoreFile.exists());
  }

  @Test
  public void testWriteAndNormalRecover()
      throws WriteProcessException, ProcessorException, InterruptedException {
    bufferwrite = new BufferWriteProcessor(directories.getFolderForTest(), deviceId, insertPath,
        parameters, SysTimeVersionController.INSTANCE,
        FileSchemaUtils.constructFileSchema(deviceId));
    for (int i = 1; i < 100; i++) {
      bufferwrite.write(deviceId, measurementId, i, dataType, String.valueOf(i));
    }
    // waiting for the end of flush
    TimeUnit.SECONDS.sleep(2);
    File dataFile = PathUtils.getBufferWriteDir(deviceId);
    // check file
    String restoreFilePath = insertPath + ".restore";
    File restoreFile = new File(dataFile, restoreFilePath);
    assertTrue(restoreFile.exists());
    BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(
        directories.getFolderForTest(), deviceId,
        insertPath, parameters, SysTimeVersionController.INSTANCE,
        FileSchemaUtils.constructFileSchema(deviceId));
    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = bufferWriteProcessor
        .queryBufferWriteData(deviceId, measurementId, dataType, Collections.emptyMap());
    assertTrue(pair.left.isEmpty());
    assertEquals(1, pair.right.size());
    ChunkMetaData chunkMetaData = pair.right.get(0);
    assertEquals(measurementId, chunkMetaData.getMeasurementUid());
    assertEquals(dataType, chunkMetaData.getTsDataType());
    bufferWriteProcessor.close();
    bufferwrite.close();
    assertFalse(restoreFile.exists());
  }

  @Test
  public void testWriteAndQuery()
      throws WriteProcessException, InterruptedException, ProcessorException {
    bufferwrite = new BufferWriteProcessor(directories.getFolderForTest(), deviceId, insertPath,
        parameters, SysTimeVersionController.INSTANCE,
        FileSchemaUtils.constructFileSchema(deviceId));
    assertFalse(bufferwrite.isFlush());
    assertTrue(bufferwrite.canBeClosed());
    assertEquals(0, bufferwrite.memoryUsage());
    assertEquals(TsFileIOWriter.magicStringBytes.length, bufferwrite.getFileSize());
    assertEquals(0, bufferwrite.getMetaSize());
    long lastFlushTime = bufferwrite.getLastFlushTime();
    for (int i = 1; i <= 85; i++) {
      bufferwrite.write(deviceId, measurementId, i, dataType, String.valueOf(i));
      assertEquals(i * 12, bufferwrite.memoryUsage());
    }
    assertEquals(lastFlushTime, bufferwrite.getLastFlushTime());
    bufferwrite.write(deviceId, measurementId, 86, dataType, String.valueOf(86));
    //assert a flush() is called.
    assertNotEquals(bufferwrite.getLastFlushTime(), lastFlushTime);
    // sleep to the end of flush
    try {
      bufferwrite.getFlushFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      //because UT uses a mock flush operation, 10 seconds should be enough.
      LOGGER.error(e.getMessage(), e);
      Assert.fail("mock flush spends more than 10 seconds... "
          + "Please modify the value or change a better test environment");
    }
    assertFalse(bufferwrite.isFlush());
    assertEquals(0, bufferwrite.memoryUsage());
    // query result
    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = bufferwrite
        .queryBufferWriteData(deviceId, measurementId,
            dataType, Collections.emptyMap());
    assertTrue(pair.left.isEmpty());
    assertEquals(1, pair.right.size());
    ChunkMetaData chunkMetaData = pair.right.get(0);
    assertEquals(measurementId, chunkMetaData.getMeasurementUid());
    assertEquals(dataType, chunkMetaData.getTsDataType());
    for (int i = 87; i <= 100; i++) {
      bufferwrite.write(deviceId, measurementId, i, dataType, String.valueOf(i));
      assertEquals((i - 86) * 12, bufferwrite.memoryUsage());
    }
    pair = bufferwrite
        .queryBufferWriteData(deviceId, measurementId, dataType, Collections.emptyMap());
    ReadOnlyMemChunk rawSeriesChunk = (ReadOnlyMemChunk) pair.left;
    assertFalse(rawSeriesChunk.isEmpty());
    assertEquals(87, rawSeriesChunk.getMinTimestamp());
    Assert.assertEquals(87, rawSeriesChunk.getValueAtMinTime().getInt());
    assertEquals(100, rawSeriesChunk.getMaxTimestamp());
    Assert.assertEquals(100, rawSeriesChunk.getValueAtMaxTime().getInt());
    Iterator<TimeValuePair> iterator = rawSeriesChunk.getIterator();
    for (int i = 87; i <= 100; i++) {
      iterator.hasNext();
      TimeValuePair timeValuePair = iterator.next();
      assertEquals(i, timeValuePair.getTimestamp());
      assertEquals(i, timeValuePair.getValue().getInt());
    }
    bufferwrite.close();
  }
}
