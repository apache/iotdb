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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.series;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;

public abstract class AbstractSeriesScanTest {

  protected static final String TEST_DATABASE = "root.sg_pd";
  protected static final IDeviceID TEST_DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_DATABASE + ".d1");
  protected static final String TEST_PATH = TEST_DEVICE.toString() + ".s1";

  /**
   * The data distribution is as follows.
   *
   * <pre>
   *  time    root.sg_pd.d1.s1
   *      ┌──────────┐
   *   0  │ f1-c1-p1 │
   *      ╞══════════╡
   *  10  │ f2-c1-p1 │
   *      │==========│
   *  20  │ f2-c2-p1 │
   *      ╞══════════╡
   *  30  │ f3-c1-p1 │
   *      │----------│
   *  40  │ f3-c1-p2 │
   *      │==========│──────────┐
   *  50  │ f3-c2-p1 │ f4-c1-p1 │
   *      └──────────│----------│
   *  60             │ f4-c1-p2 │
   *                 └──────────┘
   * </pre>
   */
  protected static final List<TsFileResource> seqResources = new ArrayList<>();

  protected static final List<TsFileResource> unSeqResources = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws IOException, WriteProcessException, IllegalPathException {
    List<PartialPath> writtenPaths = Collections.singletonList(new PartialPath(TEST_PATH));
    List<TSDataType> dataTypes = Collections.singletonList(TSDataType.INT32);
    List<TSEncoding> encodings = Collections.singletonList(TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Collections.singletonList(CompressionType.UNCOMPRESSED);

    // prepare file 1
    File seqFile1 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 1));
    TsFileResource seqFileResource1 = new TsFileResource(seqFile1);
    if (!seqFile1.getParentFile().exists()) {
      Assert.assertTrue(seqFile1.getParentFile().mkdirs());
    }
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource1.getTsFile())) {
      // prepare f1-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f1-c1-p1
      pages.add(new TimeRange(0L, 9L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource1.updateStartTime(TEST_DEVICE, 0);
      seqFileResource1.updateEndTime(TEST_DEVICE, 9);
      tsFileIOWriter.endFile();
    }
    seqFileResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource1);

    // prepare file 2
    File seqFile2 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 2));
    TsFileResource seqFileResource2 = new TsFileResource(seqFile2);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource2.getTsFile())) {
      // prepare f2-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f2-c1-p1
      pages.add(new TimeRange(10L, 19L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      // prepare f2-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      pages.clear();
      // prepare f2-c2-p1
      pages.add(new TimeRange(20L, 29L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource2.updateStartTime(TEST_DEVICE, 10);
      seqFileResource2.updateEndTime(TEST_DEVICE, 29);
      tsFileIOWriter.endFile();
    }
    seqFileResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource2);

    // prepare file 3
    File seqFile3 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 3));
    TsFileResource seqFileResource3 = new TsFileResource(seqFile3);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource3.getTsFile())) {
      // prepare f3-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f3-c1-p1
      pages.add(new TimeRange(30L, 39L));
      // prepare f3-c1-p2
      pages.add(new TimeRange(40L, 49L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      // prepare f3-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      pages.clear();
      // prepare f3-c2-p1
      pages.add(new TimeRange(50L, 59L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource3.updateStartTime(TEST_DEVICE, 30);
      seqFileResource3.updateEndTime(TEST_DEVICE, 59);
      tsFileIOWriter.endFile();
    }
    seqFileResource3.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource3);

    // prepare file 4
    File unseqFile4 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 4));
    TsFileResource unseqFileResource4 = new TsFileResource(unseqFile4);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqFileResource4.getTsFile())) {
      // prepare f4-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f4-c1-p1
      pages.add(new TimeRange(50L, 59L));
      // prepare f4-c1-p2
      pages.add(new TimeRange(60L, 69L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      unseqFileResource4.updateStartTime(TEST_DEVICE, 50);
      unseqFileResource4.updateEndTime(TEST_DEVICE, 69);
      tsFileIOWriter.endFile();
    }
    unseqFileResource4.setStatusForTest(TsFileResourceStatus.NORMAL);
    unSeqResources.add(unseqFileResource4);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unSeqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
    seqResources.clear();
    unSeqResources.clear();
    EnvironmentUtils.cleanAllDir();
  }
}
