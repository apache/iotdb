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
package org.apache.iotdb.db.storageengine.dataregion.wal.compression;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALSignalEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.LogWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALInputStream;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class WALCompressionTest {
  private final File walFile =
      new File(
          TestConstant.BASE_OUTPUT_PATH.concat(
              WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)));

  private final String compressionDir =
      TestConstant.OUTPUT_DATA_DIR.concat(File.separator + "wal-compression");

  private final String devicePath = "root.sg.d1";
  long originalMinCompressionSize;
  CompressionType originCompressionType =
      IoTDBDescriptor.getInstance().getConfig().getWALCompressionAlgorithm();

  @Before
  public void setUp()
      throws IOException, NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
    if (walFile.exists()) {
      FileUtils.delete(walFile);
    }
    originalMinCompressionSize = WALTestUtils.getMinCompressionSize();
    if (new File(compressionDir).exists()) {
      FileUtils.forceDelete(new File(compressionDir));
    }
  }

  @After
  public void tearDown()
      throws IOException, NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
    if (walFile.exists()) {
      FileUtils.delete(walFile);
    }
    if (new File(compressionDir).exists()) {
      FileUtils.forceDelete(new File(compressionDir));
    }
    WALTestUtils.setMinCompressionSize(originalMinCompressionSize);
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(originCompressionType);
  }

  @Test
  public void testSkipToGivenPositionWithCompression()
      throws NoSuchFieldException,
          ClassNotFoundException,
          IllegalAccessException,
          QueryProcessException,
          IllegalPathException,
          IOException {
    WALTestUtils.setMinCompressionSize(0L);
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(CompressionType.LZ4);
    testSkipToGivenPosition();
  }

  @Test
  public void testSkipToGivenPositionWithoutCompression()
      throws NoSuchFieldException,
          ClassNotFoundException,
          IllegalAccessException,
          QueryProcessException,
          IllegalPathException,
          IOException {
    WALTestUtils.setMinCompressionSize(1024 * 32);
    testSkipToGivenPosition();
  }

  public void testSkipToGivenPosition()
      throws QueryProcessException, IllegalPathException, IOException {
    LogWriter writer = new WALWriter(walFile);
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 4);
    List<Pair<Long, Integer>> positionAndEntryPairList = new ArrayList<>();
    int memTableId = 0;
    long fileOffset = 0;
    for (int i = 0; i < 100; ) {
      InsertRowNode insertRowNode = WALTestUtils.getInsertRowNode(devicePath + memTableId, i);
      if (buffer.remaining() >= buffer.capacity() / 4) {
        int pos = buffer.position();
        insertRowNode.serialize(buffer);
        int size = buffer.position() - pos;
        positionAndEntryPairList.add(new Pair<>(fileOffset, size));
        fileOffset += size;
        i++;
      } else {
        writer.write(buffer);
        buffer.clear();
      }
    }
    if (buffer.position() != 0) {
      writer.write(buffer);
    }
    writer.close();
    try (WALInputStream stream = new WALInputStream(walFile)) {
      for (int i = 0; i < 100; ++i) {
        Pair<Long, Integer> positionAndNodePair = positionAndEntryPairList.get(i);
        stream.skipToGivenLogicalPosition(positionAndNodePair.left);
        ByteBuffer nodeBuffer1 = ByteBuffer.allocate(positionAndNodePair.right);
        stream.read(nodeBuffer1);
        ByteBuffer nodeBuffer2 = ByteBuffer.allocate(positionAndNodePair.right);
        WALTestUtils.getInsertRowNode(devicePath + memTableId, i).serialize(nodeBuffer2);
        nodeBuffer2.flip();
        Assert.assertArrayEquals(nodeBuffer1.array(), nodeBuffer2.array());
      }
    }
  }

  @Test
  public void testUncompressedWALStructure()
      throws QueryProcessException, IllegalPathException, IOException {
    PublicBAOS baos = new PublicBAOS();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    List<InsertRowNode> insertRowNodes = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      InsertRowNode node = WALTestUtils.getInsertRowNode(devicePath, i);
      insertRowNodes.add(node);
      node.serialize(dataOutputStream);
    }
    dataOutputStream.close();
    ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
    // Do not compress it
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setWALCompressionAlgorithm(CompressionType.UNCOMPRESSED);
    try (WALWriter writer = new WALWriter(walFile)) {
      buf.position(buf.limit());
      writer.write(buf);
    }

    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(walFile.toPath())))) {
      byte[] magicStringBytes = new byte[WALWriter.MAGIC_STRING_V2_BYTES];
      // head magic string
      dataInputStream.readFully(magicStringBytes);
      Assert.assertEquals(WALWriter.MAGIC_STRING_V2, new String(magicStringBytes));
      Assert.assertEquals(
          CompressionType.UNCOMPRESSED, CompressionType.deserialize(dataInputStream.readByte()));
      Assert.assertEquals(buf.array().length, dataInputStream.readInt());
      ByteBuffer dataBuf = ByteBuffer.allocate(buf.array().length);
      dataInputStream.readFully(dataBuf.array());
      Assert.assertArrayEquals(buf.array(), dataBuf.array());
      Assert.assertEquals(
          new WALSignalEntry(WALEntryType.WAL_FILE_INFO_END_MARKER),
          WALEntry.deserialize(dataInputStream));
      ByteBuffer metadataBuf = ByteBuffer.allocate(12 + Integer.BYTES);
      dataInputStream.readFully(metadataBuf.array());
      // Tail magic string
      dataInputStream.readFully(magicStringBytes);
      Assert.assertEquals(WALWriter.MAGIC_STRING_V2, new String(magicStringBytes));
    }
  }

  @Test
  public void testCompressedWALStructure()
      throws IOException,
          QueryProcessException,
          IllegalPathException,
          NoSuchFieldException,
          ClassNotFoundException,
          IllegalAccessException {
    PublicBAOS baos = new PublicBAOS();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    List<InsertRowNode> insertRowNodes = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      InsertRowNode node = WALTestUtils.getInsertRowNode(devicePath, i);
      insertRowNodes.add(node);
      node.serialize(dataOutputStream);
    }
    dataOutputStream.close();
    ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
    // Compress it
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(CompressionType.LZ4);
    WALTestUtils.setMinCompressionSize(0);
    try (WALWriter writer = new WALWriter(walFile)) {
      writer.setCompressedByteBuffer(
          ByteBuffer.allocateDirect(WALBuffer.ONE_THIRD_WAL_BUFFER_SIZE));
      buf.position(buf.limit());
      writer.write(buf);
    }
    ICompressor compressor = ICompressor.getCompressor(CompressionType.LZ4);
    byte[] compressed = compressor.compress(buf.array());

    try (DataInputStream dataInputStream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(walFile.toPath())))) {
      byte[] magicStringBytes = new byte[WALWriter.MAGIC_STRING_V2_BYTES];
      // head magic string
      dataInputStream.readFully(magicStringBytes);
      Assert.assertEquals(WALWriter.MAGIC_STRING_V2, new String(magicStringBytes));
      Assert.assertEquals(
          CompressionType.LZ4, CompressionType.deserialize(dataInputStream.readByte()));
      Assert.assertEquals(compressed.length, dataInputStream.readInt());
      Assert.assertEquals(buf.array().length, dataInputStream.readInt());
      ByteBuffer dataBuf = ByteBuffer.allocate(compressed.length);
      dataInputStream.readFully(dataBuf.array());
      Assert.assertArrayEquals(compressed, dataBuf.array());
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.LZ4);
      Assert.assertArrayEquals(unCompressor.uncompress(compressed), buf.array());
      Assert.assertEquals(
          new WALSignalEntry(WALEntryType.WAL_FILE_INFO_END_MARKER),
          WALEntry.deserialize(dataInputStream));
      ByteBuffer metadataBuf = ByteBuffer.allocate(12 + Integer.BYTES);
      dataInputStream.readFully(metadataBuf.array());
      // Tail magic string
      dataInputStream.readFully(magicStringBytes);
      Assert.assertEquals(WALWriter.MAGIC_STRING_V2, new String(magicStringBytes));
    }
  }

  @Test
  public void testWALReaderWithoutCompression()
      throws QueryProcessException, IllegalPathException, IOException, InterruptedException {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setWALCompressionAlgorithm(CompressionType.UNCOMPRESSED);
    testWALReader();
  }

  @Test
  public void testWALReaderWithCompression()
      throws QueryProcessException,
          IllegalPathException,
          IOException,
          InterruptedException,
          NoSuchFieldException,
          ClassNotFoundException,
          IllegalAccessException {
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(CompressionType.LZ4);
    WALTestUtils.setMinCompressionSize(0);
    testWALReader();
  }

  public void testWALReader()
      throws IOException, QueryProcessException, IllegalPathException, InterruptedException {
    File dir = new File(compressionDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    WALBuffer walBuffer = new WALBuffer("", compressionDir);
    List<WALEntry> entryList = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      InsertRowNode node = WALTestUtils.getInsertRowNode(devicePath, i);
      WALEntry entry = new WALInfoEntry(0, node);
      walBuffer.write(entry);
      entryList.add(entry);
    }
    long sleepTime = 0;
    while (!walBuffer.isAllWALEntriesConsumed()) {
      Thread.sleep(100);
      sleepTime += 100;
      if (sleepTime > 10_000) {
        Assert.fail("It has been too long for all entries to be consumed");
      }
    }
    walBuffer.close();

    File[] walFiles = WALFileUtils.listAllWALFiles(new File(compressionDir));
    Assert.assertNotNull(walFiles);
    Assert.assertEquals(1, walFiles.length);
    List<WALEntry> readWALEntryList = new ArrayList<>();
    try (WALReader reader = new WALReader(walFiles[0])) {
      while (reader.hasNext()) {
        readWALEntryList.add(reader.next());
      }
    }
    Assert.assertEquals(entryList, readWALEntryList);

    try (WALByteBufReader reader = new WALByteBufReader(walFiles[0])) {
      for (int i = 0; i < 100; ++i) {
        Assert.assertTrue(reader.hasNext());
        ByteBuffer buffer = reader.next();
        Assert.assertEquals(entryList.get(i).serializedSize(), buffer.array().length);
      }
    }
  }

  @Test
  public void testHotLoad()
      throws IOException,
          QueryProcessException,
          IllegalPathException,
          InterruptedException,
          NoSuchFieldException,
          ClassNotFoundException,
          IllegalAccessException {
    File dir = new File(compressionDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    WALTestUtils.setMinCompressionSize(0);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setWALCompressionAlgorithm(CompressionType.UNCOMPRESSED);
    // Do not compress wal for these entries
    WALBuffer walBuffer = new WALBuffer("", compressionDir);
    List<WALEntry> entryList = new ArrayList<>();
    for (int i = 0; i < 50; ++i) {
      InsertRowNode node = WALTestUtils.getInsertRowNode(devicePath, i);
      WALEntry entry = new WALInfoEntry(0, node);
      walBuffer.write(entry);
      entryList.add(entry);
    }
    long sleepTime = 0;
    while (!walBuffer.isAllWALEntriesConsumed()) {
      Thread.sleep(100);
      sleepTime += 100;
      if (sleepTime > 10_000) {
        Assert.fail("It has been too long for all entries to be consumed");
      }
    }

    // compress wal for these entries
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(CompressionType.LZ4);
    for (int i = 50; i < 100; ++i) {
      InsertRowNode node = WALTestUtils.getInsertRowNode(devicePath, i);
      WALEntry entry = new WALInfoEntry(0, node);
      walBuffer.write(entry);
      entryList.add(entry);
    }
    sleepTime = 0;
    while (!walBuffer.isAllWALEntriesConsumed()) {
      Thread.sleep(100);
      sleepTime += 100;
      if (sleepTime > 10_000) {
        Assert.fail("It has been too long for all entries to be consumed");
      }
    }
    walBuffer.close();

    File[] walFiles = WALFileUtils.listAllWALFiles(new File(compressionDir));
    Assert.assertNotNull(walFiles);
    Assert.assertEquals(1, walFiles.length);
    List<WALEntry> readWALEntryList = new ArrayList<>();
    try (WALReader reader = new WALReader(walFiles[0])) {
      while (reader.hasNext()) {
        readWALEntryList.add(reader.next());
      }
    }
    Assert.assertEquals(entryList, readWALEntryList);

    try (WALByteBufReader reader = new WALByteBufReader(walFiles[0])) {
      for (int i = 0; i < 100; ++i) {
        Assert.assertTrue(reader.hasNext());
        ByteBuffer buffer = reader.next();
        Assert.assertEquals(entryList.get(i).serializedSize(), buffer.array().length);
      }
    }
  }
}
