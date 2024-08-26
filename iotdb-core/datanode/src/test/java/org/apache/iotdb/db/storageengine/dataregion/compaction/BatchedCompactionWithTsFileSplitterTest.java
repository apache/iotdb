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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.load.splitter.AlignedChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchedCompactionWithTsFileSplitterTest extends AbstractCompactionTest {

  private int originMaxConcurrentAlignedSeriesInCompaction;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    originMaxConcurrentAlignedSeriesInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(2);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(originMaxConcurrentAlignedSeriesInCompaction);
  }

  @Test
  public void testCompactionFlushChunk()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[] {new TimeRange(100000, 200000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, false),
            true);
    seqResources.add(seqResource1);
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[] {new TimeRange(300000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, false),
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource targetResource = performCompaction();
    consumeChunkDataAndValidate(targetResource);
  }

  @Test
  public void testCompactionFlushChunkAndSplitByTimePartition()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[] {new TimeRange(100000, 200000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, false),
            true);
    seqResources.add(seqResource1);
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[] {new TimeRange(604700000, 604800020)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, false),
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource targetResource = performCompaction();
    consumeChunkDataAndValidate(targetResource);
  }

  @Test
  public void testCompactionFlushPage()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10000, 20000), new TimeRange(30000, 120000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true, false, true),
            true);
    seqResources.add(seqResource1);
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(300000, 310000), new TimeRange(320000, 330000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, false),
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource targetResource = performCompaction();
    consumeChunkDataAndValidate(targetResource);
  }

  @Test
  public void testCompactionFlushPageAndSplitByTimePartition()
      throws IOException,
          StorageEngineException,
          InterruptedException,
          MetadataException,
          PageException {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(10000, 20000), new TimeRange(30000, 120000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, true),
            true);
    seqResources.add(seqResource1);
    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2", "s3", "s4"),
            new TimeRange[][] {
              new TimeRange[] {
                new TimeRange(604799900, 604800020), new TimeRange(604810020, 604820020)
              }
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false, false, false),
            true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource targetResource = performCompaction();
    consumeChunkDataAndValidate(targetResource);
  }

  private TsFileResource performCompaction()
      throws StorageEngineException,
          IOException,
          PageException,
          InterruptedException,
          MetadataException {
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(seqResources, true);

    ReadChunkCompactionPerformer performer = new ReadChunkCompactionPerformer();
    CompactionTaskSummary summary = new CompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(seqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
    return targetResource;
  }

  private void consumeChunkDataAndValidate(TsFileResource resource)
      throws IOException, IllegalPathException {
    Map<TTimePartitionSlot, TestLoadTsFileIOWriter> writerMap = new HashMap<>();

    TsFileSplitter splitter =
        new TsFileSplitter(
            resource.getTsFile(),
            tsFileData -> {
              AlignedChunkData alignedChunkData = (AlignedChunkData) tsFileData;
              TestLoadTsFileIOWriter writer =
                  writerMap.computeIfAbsent(
                      alignedChunkData.getTimePartitionSlot(),
                      slot -> {
                        try {
                          return new TestLoadTsFileIOWriter(createEmptyFileAndResource(false));
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      });
              try {
                IDeviceID deviceID =
                    IDeviceID.Factory.DEFAULT_FACTORY.create(alignedChunkData.getDevice());
                if (!deviceID.equals(writer.currentDevice)) {
                  if (writer.currentDevice != null) {
                    writer.endChunkGroup();
                  }
                  writer.startChunkGroup(deviceID);
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                alignedChunkData.serialize(dos);
                AlignedChunkData newAlignedChunkData =
                    (AlignedChunkData)
                        TsFileData.deserialize(new ByteArrayInputStream(baos.toByteArray()));
                newAlignedChunkData.writeToFileWriter(writer);
              } catch (IOException | PageException | IllegalPathException e) {
                throw new RuntimeException(e);
              }
              return true;
            });
    splitter.splitTsFileByDataPartition();
    List<TsFileResource> splitResources = new ArrayList<>();
    for (Map.Entry<TTimePartitionSlot, TestLoadTsFileIOWriter> entry : writerMap.entrySet()) {
      TestLoadTsFileIOWriter writer = entry.getValue();
      writer.endChunkGroup();
      writer.endFile();
      writer.close();
      writer.resource.serialize();
      splitResources.add(writer.resource);
      TsFileResourceUtils.validateTsFileDataCorrectness(writer.resource);
    }

    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(resource)),
                Collections.singletonList(resource),
                Collections.emptyList()),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(splitResources), Collections.emptyList(), splitResources)));
  }

  private static class TestLoadTsFileIOWriter extends TsFileIOWriter {
    private IDeviceID currentDevice;
    private TsFileResource resource;

    public TestLoadTsFileIOWriter(TsFileResource resource) throws IOException {
      super(resource.getTsFile());
      this.resource = resource;
    }

    @Override
    public int startChunkGroup(IDeviceID deviceId) throws IOException {
      this.currentDevice = deviceId;
      return super.startChunkGroup(deviceId);
    }

    @Override
    public void endChunkGroup() throws IOException {
      for (ChunkMetadata chunkMetadata : getChunkMetadataListOfCurrentDeviceInMemory()) {
        if (!"".equals(chunkMetadata.getMeasurementUid())) {
          continue;
        }
        resource.updateStartTime(currentDevice, chunkMetadata.getStartTime());
        resource.updateEndTime(currentDevice, chunkMetadata.getEndTime());
      }
      super.endChunkGroup();
    }
  }
}
