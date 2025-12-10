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

package org.apache.iotdb.db.storageengine.dataregion.compaction.object;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.Base32ObjectPath;
import org.apache.iotdb.db.storageengine.dataregion.IObjectPath;
import org.apache.iotdb.db.storageengine.dataregion.PlainObjectPath;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.ObjectTypeUtils;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

public class ObjectTypeCompactionTest extends AbstractCompactionTest {

  private static final TableSchema tableSchema =
      new TableSchema(
          "t1",
          Arrays.asList(
              new ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
              new ColumnSchema("s1", TSDataType.OBJECT, ColumnCategory.FIELD)));

  private String threadName;
  private File objectDir;
  private File regionDir;

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  @Override
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    config.setRestrictObjectLimit(true);
    this.threadName = Thread.currentThread().getName();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
    DataNodeTableCache.getInstance().invalid(this.COMPACTION_TEST_SG);
    createTable("t1", 1);
    super.setUp();
    try {
      objectDir = new File(TierManager.getInstance().getNextFolderForObjectFile());
      regionDir = new File(objectDir, "0");
      regionDir.mkdirs();
    } catch (DiskSpaceInsufficientException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  @Override
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(threadName);
    DataNodeTableCache.getInstance().invalid(this.COMPACTION_TEST_SG);
    File[] files = objectDir.listFiles();
    if (files != null) {
      for (File file : files) {
        FileUtils.deleteFileOrDirectory(file);
      }
    }
    config.setRestrictObjectLimit(false);
  }

  public void createTable(String tableName, long ttl) {
    TsTable tsTable = new TsTable(tableName);
    tsTable.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    tsTable.addColumnSchema(
        new FieldColumnSchema("s1", TSDataType.OBJECT, TSEncoding.PLAIN, CompressionType.LZ4));
    tsTable.addProp(TsTable.TTL_PROPERTY, ttl + "");
    DataNodeTableCache.getInstance().preUpdateTable(this.COMPACTION_TEST_SG, tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable(this.COMPACTION_TEST_SG, tableName, null);
  }

  @Test
  public void testSeqCompactionWithTTL() throws IOException, WriteProcessException {
    Pair<TsFileResource, File> pair1 =
        generateTsFileAndObject(true, System.currentTimeMillis() - 10000, 0);
    Pair<TsFileResource, File> pair2 =
        generateTsFileAndObject(true, System.currentTimeMillis() + 1000000, 100);
    tsFileManager.add(pair1.getLeft(), true);
    tsFileManager.add(pair2.getLeft(), true);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            tsFileManager.getTsFileList(true),
            true,
            new ReadChunkCompactionPerformer(),
            0);
    Assert.assertTrue(task.start());
    Assert.assertFalse(pair1.getRight().exists());
    Assert.assertTrue(pair2.getRight().exists());
  }

  @Test
  public void testUnseqCompactionWithTTL() throws IOException, WriteProcessException {
    Pair<TsFileResource, File> pair1 =
        generateTsFileAndObject(false, System.currentTimeMillis() + 100000, 1);
    Pair<TsFileResource, File> pair2 =
        generateTsFileAndObject(false, System.currentTimeMillis() - 1000000, 0);
    tsFileManager.add(pair1.getLeft(), false);
    tsFileManager.add(pair2.getLeft(), false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            tsFileManager.getTsFileList(false),
            false,
            new FastCompactionPerformer(false),
            0);
    Assert.assertTrue(task.start());
    Assert.assertFalse(pair2.getRight().exists());
    Assert.assertTrue(pair1.getRight().exists());
  }

  @Test
  public void testUnseqCompactionWithReadPointWithTTL() throws IOException, WriteProcessException {
    Pair<TsFileResource, File> pair1 =
        generateTsFileAndObject(false, System.currentTimeMillis() + 100000, 0);
    Pair<TsFileResource, File> pair2 =
        generateTsFileAndObject(false, System.currentTimeMillis() - 1000000, 0);
    tsFileManager.add(pair1.getLeft(), false);
    tsFileManager.add(pair2.getLeft(), false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            tsFileManager.getTsFileList(false),
            false,
            new ReadPointCompactionPerformer(),
            0);
    Assert.assertTrue(task.start());
    Assert.assertTrue(pair1.getRight().exists());
    Assert.assertFalse(pair2.getRight().exists());
  }

  @Test
  public void testCrossCompactionWithTTL() throws IOException, WriteProcessException {
    Pair<TsFileResource, File> pair1 =
        generateTsFileAndObject(true, System.currentTimeMillis() + 100000, 1);
    Pair<TsFileResource, File> pair2 =
        generateTsFileAndObject(false, System.currentTimeMillis() - 1000000, 2);
    tsFileManager.add(pair1.getLeft(), true);
    tsFileManager.add(pair2.getLeft(), false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            tsFileManager.getTsFileList(true),
            tsFileManager.getTsFileList(false),
            new FastCompactionPerformer(true),
            1,
            0);
    Assert.assertTrue(task.start());
    Assert.assertFalse(pair2.getRight().exists());
    Assert.assertTrue(pair1.getRight().exists());
  }

  @Test
  public void testSettleCompaction() throws IOException, WriteProcessException {
    Pair<TsFileResource, File> pair1 =
        generateTsFileAndObject(true, System.currentTimeMillis() - 10000, 3);
    Pair<TsFileResource, File> pair2 =
        generateTsFileAndObject(true, System.currentTimeMillis() + 1000000, 0);
    tsFileManager.add(pair1.getLeft(), true);
    tsFileManager.add(pair2.getLeft(), true);
    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            tsFileManager.getTsFileList(true),
            Collections.emptyList(),
            true,
            new FastCompactionPerformer(true),
            0);
    Assert.assertTrue(task.start());
    Assert.assertFalse(pair1.getRight().exists());
    Assert.assertTrue(pair2.getRight().exists());
  }

  @Test
  public void testPlainObjectBinaryReplaceRegionId() {
    IObjectPath objectPath = new PlainObjectPath(1, 0, new StringArrayDeviceID("t1.d1"), "s1");
    ByteBuffer buffer =
        ByteBuffer.allocate(Long.BYTES + objectPath.getSerializeSizeToObjectValue());
    buffer.putLong(10);
    objectPath.serializeToObjectValue(buffer);

    Binary origin = new Binary(buffer.array());
    Binary result = ObjectTypeUtils.replaceRegionIdForObjectBinary(10, origin);
    ByteBuffer deserializeBuffer = ByteBuffer.wrap(result.getValues());
    deserializeBuffer.getLong();
    Assert.assertEquals(
        new PlainObjectPath(10, 0, new StringArrayDeviceID("t1.d1"), "s1").toString(),
        IObjectPath.getDeserializer().deserializeFromObjectValue(deserializeBuffer).toString());
  }

  @Test
  public void testBase32ObjectBinaryReplaceRegionId() {
    config.setRestrictObjectLimit(false);
    try {
      IObjectPath objectPath = new Base32ObjectPath(1, 0, new StringArrayDeviceID("t1.d1"), "s1");
      ByteBuffer buffer =
          ByteBuffer.allocate(Long.BYTES + objectPath.getSerializeSizeToObjectValue());
      buffer.putLong(10);
      objectPath.serializeToObjectValue(buffer);

      Binary origin = new Binary(buffer.array());
      Binary result = ObjectTypeUtils.replaceRegionIdForObjectBinary(10, origin);
      ByteBuffer deserializeBuffer = ByteBuffer.wrap(result.getValues());
      deserializeBuffer.getLong();
      Assert.assertEquals(
          new Base32ObjectPath(10, 0, new StringArrayDeviceID("t1.d1"), "s1").toString(),
          IObjectPath.getDeserializer().deserializeFromObjectValue(deserializeBuffer).toString());
    } finally {
      config.setRestrictObjectLimit(true);
    }
  }

  private Pair<TsFileResource, File> generateTsFileAndObject(
      boolean seq, long timestamp, int regionIdInTsFile) throws IOException, WriteProcessException {
    TsFileResource resource = createEmptyFileAndResource(seq);
    Path testFile1 = Files.createTempFile(regionDir.toPath(), "test_", ".bin");
    byte[] content = new byte[100];
    for (int i = 0; i < 100; i++) {
      content[i] = (byte) i;
    }
    Files.write(testFile1, content);
    String relativePathInTsFile = regionIdInTsFile + File.separator + testFile1.toFile().getName();
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + relativePathInTsFile.length());
    buffer.putLong(100L);
    buffer.put(BytesUtils.stringToBytes(relativePathInTsFile));
    buffer.flip();
    IDeviceID deviceID = new StringArrayDeviceID("t1", "d1");
    try (TsFileIOWriter writer = new TsFileIOWriter(resource.getTsFile())) {
      writer.getSchema().registerTableSchema(tableSchema);
      writer.startChunkGroup(deviceID);
      AlignedChunkWriterImpl alignedChunkWriter =
          new AlignedChunkWriterImpl(Arrays.asList(new MeasurementSchema("s1", TSDataType.OBJECT)));
      alignedChunkWriter.write(timestamp);
      alignedChunkWriter.write(timestamp, new Binary(buffer.array()), false);
      alignedChunkWriter.sealCurrentPage();
      alignedChunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
    resource.updateStartTime(deviceID, 1);
    resource.updateEndTime(deviceID, 1);
    resource.serialize();
    resource.deserialize();
    resource.setStatus(TsFileResourceStatus.NORMAL);
    return new Pair<>(resource, testFile1.toFile());
  }
}
