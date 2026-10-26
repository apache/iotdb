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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.LoadTsFileManager;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TsFileSplitConsumerTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDeletionThatExceedsMemoryBudgetTriggersDispatch() throws Exception {
    final int memoryBudget = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize() >> 2;
    final Constructor<LoadTsFileDataCacheMemoryBlock> constructor =
        LoadTsFileDataCacheMemoryBlock.class.getDeclaredConstructor(long.class);
    constructor.setAccessible(true);
    final LoadTsFileDataCacheMemoryBlock memoryBlock = constructor.newInstance(1024 * 1024L);

    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet()
            .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    final DataPartitionBatchFetcher partitionFetcher =
        Mockito.mock(DataPartitionBatchFetcher.class);
    when(partitionFetcher.queryDataPartition(anyList(), anyString()))
        .thenReturn(List.of(replicaSet));

    final LoadSingleTsFileNode loadNode = Mockito.mock(LoadSingleTsFileNode.class);
    when(loadNode.getPlanNodeId()).thenReturn(new PlanNodeId("load-memory-budget"));
    final TsFileResource resource = Mockito.mock(TsFileResource.class);
    when(resource.getTsFile()).thenReturn(new File("load-memory-budget.tsfile"));
    when(loadNode.getTsFileResource()).thenReturn(resource);

    final AtomicInteger dispatchedPieces = new AtomicInteger();
    final TsFileSplitConsumer consumer =
        new TsFileSplitConsumer(
            loadNode,
            memoryBlock,
            new DataPartitionResolver(partitionFetcher, "root.test"),
            null,
            (pieceNode, targetReplicaSet) -> {
              if (pieceNode.getAllTsFileData().stream().anyMatch(DeletionData.class::isInstance)) {
                dispatchedPieces.incrementAndGet();
              }
              return true;
            });

    final NonAlignedChunkData chunk =
        new NonAlignedChunkData(
            new StringArrayDeviceID("root", "test", "d1"),
            new ChunkHeader(
                "s1", 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0),
            new TTimePartitionSlot(0L));
    chunk.writeDecodePage(new long[] {1L}, new Object[] {1}, 1);
    chunk.endChunk();

    final ModEntry deletion = Mockito.mock(ModEntry.class);
    when(deletion.serializedSize()).thenReturn(memoryBudget + 1);

    try {
      Assert.assertTrue(consumer.apply(chunk));
      Assert.assertEquals(0, dispatchedPieces.get());

      Assert.assertTrue(consumer.apply(new DeletionData(deletion)));

      Assert.assertEquals(
          "A deletion must trigger largest-first dispatch when its buffered piece exceeds budget",
          1,
          dispatchedPieces.get());
    } finally {
      consumer.clear();
    }
  }

  @Test
  public void testDeletionsAreSentOncePerRegionAfterAllPartitions() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[] originalLoadDirs = config.getLoadTsFileDirs();
    final File loadDir = temporaryFolder.newFolder("load");
    config.setLoadTsFileDirs(new String[] {loadDir.getAbsolutePath()});
    final LoadTsFileManager firstRegion = manager(1);
    final LoadTsFileManager secondRegion = manager(2);
    final Map<Integer, LoadTsFileManager> managers = Map.of(1, firstRegion, 2, secondRegion);
    final TRegionReplicaSet firstReplicaSet = replicaSet(1);
    final TRegionReplicaSet secondReplicaSet = replicaSet(2);
    final DataPartitionBatchFetcher partitionFetcher =
        Mockito.mock(DataPartitionBatchFetcher.class);
    when(partitionFetcher.queryDataPartition(anyList(), anyString()))
        .thenAnswer(
            invocation -> {
              final List<Pair<org.apache.tsfile.file.metadata.IDeviceID, TTimePartitionSlot>>
                  slots = invocation.getArgument(0);
              return slots.stream()
                  .map(
                      slot ->
                          slot.right.getStartTime() < 2000L ? firstReplicaSet : secondReplicaSet)
                  .toList();
            });
    final Constructor<LoadTsFileDataCacheMemoryBlock> constructor =
        LoadTsFileDataCacheMemoryBlock.class.getDeclaredConstructor(long.class);
    constructor.setAccessible(true);
    final TsFileSplitConsumer consumer =
        new TsFileSplitConsumer(
            loadNode(),
            constructor.newInstance(1024 * 1024L),
            new DataPartitionResolver(partitionFetcher, "root"),
            null,
            (piece, target) -> {
              try {
                managers
                    .get(target.getRegionId().getId())
                    .writePiece("load-deletions", piece.getAllTsFileData());
                return true;
              } catch (Exception e) {
                throw new AssertionError(e);
              }
            });
    final ModEntry firstDeletion =
        new TreeDeletionEntry(new MeasurementPath("root.test.d1.s1"), 0L, 10L);
    final ModEntry secondDeletion =
        new TreeDeletionEntry(new MeasurementPath("root.test.d1.s1"), 20L, 30L);
    try {
      for (long partition : new long[] {0L, 1000L, 2000L}) {
        Assert.assertTrue(consumer.apply(chunk(partition)));
      }
      Assert.assertTrue(consumer.apply(new DeletionData(firstDeletion)));
      Assert.assertTrue(consumer.apply(new DeletionData(secondDeletion)));
      Assert.assertTrue(consumer.sendAllTsFileData());
      for (long partition : new long[] {0L, 1000L, 2000L}) {
        final int regionId = partition < 2000L ? 1 : 2;
        final File taskDir = managers.get(regionId).getActiveTaskDirs().get(0);
        final File staged =
            new File(taskDir, "root.test-" + regionId + "-" + partition + ".tsfile");
        Assert.assertEquals(
            "Each partition must receive every deletion exactly once",
            List.of(firstDeletion, secondDeletion),
            ModificationFile.readAllModifications(staged, false));
      }
    } finally {
      consumer.clear();
      firstRegion.stop();
      secondRegion.stop();
      config.setLoadTsFileDirs(originalLoadDirs);
    }
  }

  private static LoadTsFileManager manager(final int regionId) {
    final DataRegion region = Mockito.mock(DataRegion.class);
    when(region.getDatabaseName()).thenReturn("root.test");
    when(region.getDataRegionIdString()).thenReturn(String.valueOf(regionId));
    return new LoadTsFileManager(region);
  }

  private static TRegionReplicaSet replicaSet(final int regionId) {
    return new TRegionReplicaSet()
        .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId));
  }

  private static LoadSingleTsFileNode loadNode() {
    final LoadSingleTsFileNode node = Mockito.mock(LoadSingleTsFileNode.class);
    when(node.getPlanNodeId()).thenReturn(new PlanNodeId("load-deletions"));
    final TsFileResource resource = Mockito.mock(TsFileResource.class);
    when(resource.getTsFile()).thenReturn(new File("load-deletions.tsfile"));
    when(node.getTsFileResource()).thenReturn(resource);
    return node;
  }

  private static NonAlignedChunkData chunk(final long partition) {
    final NonAlignedChunkData chunk =
        new NonAlignedChunkData(
            new StringArrayDeviceID("root", "test", "d1"),
            new ChunkHeader(
                "s1", 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0),
            new TTimePartitionSlot(partition));
    chunk.writeDecodePage(new long[] {partition + 1}, new Object[] {1}, 1);
    chunk.endChunk();
    return chunk;
  }
}
