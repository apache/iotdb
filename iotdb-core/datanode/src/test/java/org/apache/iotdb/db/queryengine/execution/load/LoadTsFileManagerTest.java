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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId.Factory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LoadTsFileManagerTest extends TestBase {

  private LoadTsFileManager loadTsFileManager = new LoadTsFileManager();
  private long maxSplitSize = 128 * 1024 * 1024;

  @Override
  public void setup() throws IOException, WriteProcessException, DataRegionException {
    fileNum = 10;
    seriesNum = 100;
    deviceNum = 100;
    super.setup();
  }

  @Test
  public void test() throws IOException {

    LoadTsFileNode loadTsFileNode =
        new LoadTsFileNode(new PlanNodeId("testPlanNode"), tsFileResources);
    DataPartitionBatchFetcher partitionBatchFetcher = dummyDataPartitionBatchFetcher();
    TsFileSplitSender splitSender =
        new TsFileSplitSender(
            loadTsFileNode,
            partitionBatchFetcher,
            TimePartitionUtils.getTimePartitionInterval(),
            internalServiceClientManager,
            false,
            maxSplitSize,
            100,
            "root",
            "root");
    long start = System.currentTimeMillis();
    splitSender.start();
    long timeConsumption = System.currentTimeMillis() - start;

    System.out.printf("Split ends after %dms\n", timeConsumption);

    ConsensusGroupId d1GroupId = Factory.create(TConsensusGroupType.DataRegion.getValue(), 0);
    DataRegion dataRegion = dataRegionMap.get(d1GroupId.convertToTConsensusGroupId());
    List<TsFileResource> tsFileList = dataRegion.getTsFileManager().getTsFileList(false);
    // all input files should be shallow-merged into one
    System.out.printf("Loaded TsFiles: %s\n", tsFileList);
    assertEquals(1, tsFileList.size());

    long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
    long chunkTimeRange = (long) (timePartitionInterval * chunkTimeRangeRatio);
    int chunkPointNum = (int) (chunkTimeRange / pointInterval);
    long endTime = chunkTimeRange * (fileNum - 1) + pointInterval * (chunkPointNum - 1);
    // check device time
    TsFileResource tsFileResource = tsFileList.get(0);
    for (int i = 0; i < deviceNum; i++) {
      assertEquals(0, tsFileResource.getStartTime("d" + i));
      assertEquals(endTime, tsFileResource.getEndTime("d" + i));
    }

    // read and check the generated file
    try (TsFileReader reader =
        new TsFileReader(new TsFileSequenceReader(tsFileResource.getTsFile().getPath()))) {
      for (int dn = 0; dn < deviceNum; dn++) {
        // "Simple_" is generated with linear function and is easy to check
        QueryExpression queryExpression =
            QueryExpression.create(
                Collections.singletonList(new Path("d" + dn, "Simple_22", false)), null);
        QueryDataSet dataSet = reader.query(queryExpression);
        int i = 0;
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          long currTime =
              chunkTimeRange * (i / chunkPointNum) + pointInterval * (i % chunkPointNum);
          assertEquals(currTime, record.getTimestamp());
          assertEquals(1.0 * (i % chunkPointNum), record.getFields().get(0).getDoubleV(), 0.0001);
          i++;
        }
        assertEquals(chunkPointNum * fileNum, i);
      }
    }
  }

  public TLoadResp handleTsFilePieceNode(TTsFilePieceReq req, TEndPoint tEndpoint)
      throws TException, IOException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    ByteBuffer buf = req.body.slice();
    if (req.isSetCompressionType()) {
      CompressionType compressionType = CompressionType.deserialize(req.compressionType);
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
      int uncompressedLength = req.getUncompressedLength();
      ByteBuffer allocate = ByteBuffer.allocate(uncompressedLength);
      unCompressor.uncompress(
          buf.array(), buf.arrayOffset() + buf.position(), buf.remaining(), allocate.array(), 0);
      allocate.limit(uncompressedLength);
      buf = allocate;
    }

    LoadTsFilePieceNode pieceNode = (LoadTsFilePieceNode) PlanNodeType.deserialize(buf);
    loadTsFileManager.writeToDataRegion(
        dataRegionMap.get(req.consensusGroupId), pieceNode, req.uuid);

    // forward to other replicas in the group
    if (req.relayTargets != null) {
      TRegionReplicaSet regionReplicaSet = req.relayTargets;
      req.relayTargets = null;
      regionReplicaSet.getDataNodeLocations().stream()
          .parallel()
          .forEach(
              dataNodeLocation -> {
                TEndPoint otherPoint = dataNodeLocation.getInternalEndPoint();
                if (!otherPoint.equals(tEndpoint)) {
                  try {
                    handleTsFilePieceNode(req, otherPoint);
                  } catch (TException | IOException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
    }

    return new TLoadResp()
        .setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public TLoadResp handleTsLoadCommand(TLoadCommandReq req, TEndPoint tEndpoint)
      throws LoadFileException, IOException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);

    // forward to other replicas in the group
    if (req.useConsensus) {
      req.useConsensus = false;
      TRegionReplicaSet regionReplicaSet = groupId2ReplicaSetMap.get(groupId);
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint otherPoint = dataNodeLocation.getInternalEndPoint();
        if (!otherPoint.equals(tEndpoint)) {
          handleTsLoadCommand(req, otherPoint);
        }
      }
    }

    loadTsFileManager.load(req.uuid, null, false);

    return new TLoadResp()
        .setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }
}
