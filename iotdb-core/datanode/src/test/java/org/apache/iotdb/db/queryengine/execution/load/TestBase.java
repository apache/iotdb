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

import static org.apache.iotdb.commons.conf.IoTDBConstant.GB;

import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId.Factory;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.utils.SequenceUtils.DoubleSequenceGenerator;
import org.apache.iotdb.db.utils.SequenceUtils.DoubleSequenceGeneratorFactory;
import org.apache.iotdb.db.utils.SequenceUtils.GaussianDoubleSequenceGenerator;
import org.apache.iotdb.db.utils.SequenceUtils.SimpleDoubleSequenceGenerator;
import org.apache.iotdb.db.utils.SequenceUtils.UniformDoubleSequenceGenerator;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TestBase.class);
  public static final String BASE_OUTPUT_PATH = "target".concat(File.separator).concat("loadTest");
  public static final String PARTIAL_PATH_STRING =
      "%s" + File.separator + "%d" + File.separator + "%d" + File.separator;
  public static final String TEST_TSFILE_PATH =
      BASE_OUTPUT_PATH + File.separator + "testTsFile".concat(File.separator) + PARTIAL_PATH_STRING;

  protected int fileNum = 100;
  // series number of each file, sn non-aligned series and 1 aligned series with sn measurements
  protected int seriesNum = 1000;
  protected int deviceNum = 100;
  // number of chunks of each series in a file, each series has only one chunk in a file
  protected double chunkTimeRangeRatio = 0.001;
  // the interval between two consecutive points of a series
  protected long pointInterval = 50_000;
  protected List<Pair<String, DoubleSequenceGeneratorFactory>> measurementSequenceGeneratorPairs = Arrays.asList(
      new Pair<>("Simple_", new SimpleDoubleSequenceGenerator.Factory()),
      new Pair<>("UniformA_", new UniformDoubleSequenceGenerator.Factory(1.0)),
      new Pair<>("GaussianA_", new GaussianDoubleSequenceGenerator.Factory(1.0, 1.0)),
      new Pair<>("UniformB_", new UniformDoubleSequenceGenerator.Factory(10.0)),
      new Pair<>("GaussianB_", new GaussianDoubleSequenceGenerator.Factory(15.0, 3.0))
  );
  protected final List<File> files = new ArrayList<>();
  protected final List<TsFileResource> tsFileResources = new ArrayList<>();
  protected IPartitionFetcher partitionFetcher;
  // the key is deviceId, not partitioned by time in the simple test
  protected Map<String, TRegionReplicaSet> partitionTable = new HashMap<>();
  protected Map<TConsensusGroupId, DataRegion> dataRegionMap = new HashMap<>();
  protected Map<ConsensusGroupId, TRegionReplicaSet> groupId2ReplicaSetMap = new HashMap<>();
  protected IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;
  // the third key is UUid, the forth key is targetFile

  private int groupSizeInByte;

  @Before
  public void setup() throws IOException, WriteProcessException, DataRegionException {
    setupFiles();
    logger.info("{} files set up", files.size());
    partitionFetcher = dummyPartitionFetcher();
    setupPartitionTable();
    setupClientManager();
    groupSizeInByte = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte((int) (GB));
  }

  @After
  public void cleanup() {
    FileUtils.deleteDirectory(new File(BASE_OUTPUT_PATH));
    FileUtils.deleteDirectory(new File("target" + File.separator + "data"));
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(groupSizeInByte);
  }

  public int expectedChunkNum() {
    double totalTimeRange = chunkTimeRangeRatio * fileNum;
    int splitChunkNum = 0;
    // if the boundary of the ith partition does not overlap a chunk, it introduces an additional
    // split
    // TODO: due to machine precision, the calculation may have error. Also, if the data amount is
    // too large, there could be more than one chunk for each series in the file.
    for (int i = 0; i <= totalTimeRange; i++) {
      if (i * 1.0 % chunkTimeRangeRatio > 0.00001) {
        splitChunkNum += 1;
      }
    }
    return (splitChunkNum + fileNum) * seriesNum * 2;
  }

  public TLoadResp handleTsFilePieceNode(TTsFilePieceReq req, TEndPoint tEndpoint)
      throws TException, IOException {
    return new TLoadResp()
        .setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public TLoadResp handleTsLoadCommand(TLoadCommandReq req, TEndPoint tEndpoint)
      throws LoadFileException, IOException {
    return new TLoadResp()
        .setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public TProtocol dummyProtocol() throws TTransportException {
    return new TBinaryProtocol(new TByteBuffer(ByteBuffer.allocate(0)));
  }

  public void setupClientManager() {
    SyncDataNodeInternalServiceClientPoolFactory poolFactory =
        new SyncDataNodeInternalServiceClientPoolFactory();
    internalServiceClientManager =
        new ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>(poolFactory) {
          @Override
          public SyncDataNodeInternalServiceClient borrowClient(TEndPoint node) {
            try {
              return new SyncDataNodeInternalServiceClient(
                  dummyProtocol(), new ThriftClientProperty.Builder().build(), node, this) {
                @Override
                public TLoadResp sendTsFilePieceNode(TTsFilePieceReq req) throws TException {
                  try {
                    return handleTsFilePieceNode(req, getTEndpoint());
                  } catch (IOException e) {
                    throw new TException(e);
                  }
                }

                @Override
                public TLoadResp sendLoadCommand(TLoadCommandReq req) throws TException {
                  try {
                    return handleTsLoadCommand(req, getTEndpoint());
                  } catch (LoadFileException | IOException e) {
                    throw new TException(e);
                  }
                }

                @Override
                public void close() {
                }
              };
            } catch (TTransportException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void clear(TEndPoint node) {
          }

          @Override
          public void close() {
          }
        };
  }

  public void setupPartitionTable() throws DataRegionException {
    ConsensusGroupId d1GroupId = Factory.create(TConsensusGroupType.DataRegion.getValue(), 0);
    TRegionReplicaSet d1Replicas =
        new TRegionReplicaSet(
            d1GroupId.convertToTConsensusGroupId(),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(0)
                    .setInternalEndPoint(new TEndPoint("localhost", 10000)),
                new TDataNodeLocation()
                    .setDataNodeId(1)
                    .setInternalEndPoint(new TEndPoint("localhost", 10001)),
                new TDataNodeLocation()
                    .setDataNodeId(2)
                    .setInternalEndPoint(new TEndPoint("localhost", 10002))));

    WALRecoverManager.getInstance()
        .setAllDataRegionScannedLatch(new CountDownLatch(0));
    DataRegion dataRegion = new DataRegion(BASE_OUTPUT_PATH, d1GroupId.toString(),
        new DirectFlushPolicy(), "root.loadTest");
    for (int i = 0; i < deviceNum; i++) {
      partitionTable.put("d" + i, d1Replicas);
      dataRegionMap.put(d1GroupId.convertToTConsensusGroupId(), dataRegion);
    }

    groupId2ReplicaSetMap.put(d1GroupId, d1Replicas);
    ConsensusGroupId d2GroupId = Factory.create(TConsensusGroupType.DataRegion.getValue(), 1);
    TRegionReplicaSet d2Replicas =
        new TRegionReplicaSet(
            d2GroupId.convertToTConsensusGroupId(),
            Arrays.asList(
                new TDataNodeLocation()
                    .setDataNodeId(3)
                    .setInternalEndPoint(new TEndPoint("localhost", 10003)),
                new TDataNodeLocation()
                    .setDataNodeId(4)
                    .setInternalEndPoint(new TEndPoint("localhost", 10004)),
                new TDataNodeLocation()
                    .setDataNodeId(5)
                    .setInternalEndPoint(new TEndPoint("localhost", 10005))));
    partitionTable.put("dd1", d2Replicas);
    groupId2ReplicaSetMap.put(d2GroupId, d2Replicas);
  }

  public IPartitionFetcher dummyPartitionFetcher() {
    return new IPartitionFetcher() {
      @Override
      public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
        return null;
      }

      @Override
      public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
        return null;
      }

      @Override
      public DataPartition getDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return null;
      }

      @Override
      public DataPartition getDataPartitionWithUnclosedTimeRange(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return null;
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return null;
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          List<DataPartitionQueryParam> dataPartitionQueryParams) {
        return null;
      }

      @Override
      public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
          PathPatternTree patternTree, Integer level) {
        return null;
      }

      @Override
      public boolean updateRegionCache(TRegionRouteReq req) {
        return false;
      }

      @Override
      public void invalidAllCache() {
      }
    };
  }

  public DataPartitionBatchFetcher dummyDataPartitionBatchFetcher() {
    return new DataPartitionBatchFetcher(partitionFetcher) {
      @Override
      public List<TRegionReplicaSet> queryDataPartition(
          List<Pair<String, TTimePartitionSlot>> slotList) {
        return slotList.stream().map(p -> partitionTable.get(p.left)).collect(Collectors.toList());
      }
    };
  }

  public void setupFiles() {
    measurementSequenceGeneratorPairs.sort(Comparator.comparing(Pair::getLeft));
    List<Pair<MeasurementSchema, DoubleSequenceGeneratorFactory>> schemaGeneratorPairs = new ArrayList<>();
    for (int sn = 0; sn < seriesNum; sn++) {
      Pair<String, DoubleSequenceGeneratorFactory> measurementGeneratorPair = measurementSequenceGeneratorPairs.get(
          sn % measurementSequenceGeneratorPairs.size());
      MeasurementSchema measurementSchema =
          new MeasurementSchema(measurementGeneratorPair
              .left + sn,
              TSDataType.DOUBLE);
      measurementSchema.setCompressor(CompressionType.LZ4.serialize());
      measurementSchema.setEncoding(TSEncoding.PLAIN.serialize());
      schemaGeneratorPairs.add(new Pair<>(measurementSchema, measurementGeneratorPair.right));
    }
    schemaGeneratorPairs.sort(Comparator.comparing(s -> s.left.getMeasurementId()));
    List<MeasurementSchema> measurementSchemas = schemaGeneratorPairs.stream().map(m -> m.left)
        .collect(
            Collectors.toList());
    IntStream.range(0, fileNum)
        .parallel()
        .forEach(
            i -> {
              try {
                File file = new File(getTestTsFilePath("root.sg1", 0, 0, i));
                TsFileResource tsFileResource = new TsFileResource(file);
                synchronized (files) {
                  files.add(file);
                }

                try (TsFileWriter writer = new TsFileWriter(file)) {
                  // sn non-aligned series under d1 and 1 aligned series with sn measurements under
                  // dd2
                  for (int sn = 0; sn < seriesNum; sn++) {
                    for (int dn = 0; dn < deviceNum; dn++) {
                      writer.registerTimeseries(new Path("d" + dn),
                          schemaGeneratorPairs.get(sn).left);
                    }
                  }
                  writer.registerAlignedTimeseries(new Path("dd1"), measurementSchemas);

                  // one chunk for each series
                  long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
                  long chunkTimeRange = (long) (timePartitionInterval * chunkTimeRangeRatio);
                  int chunkPointNum = (int) (chunkTimeRange / pointInterval);

                  Tablet tablet = new Tablet("d0", measurementSchemas, chunkPointNum);
                  for (int sn = 0; sn < seriesNum; sn++) {
                    DoubleSequenceGenerator sequenceGenerator =
                        schemaGeneratorPairs.get(sn).right
                            .create();
                    for (int pn = 0; pn < chunkPointNum; pn++) {
                      if (sn == 0) {
                        long currTime = chunkTimeRange * i + pointInterval * pn;
                        tablet.addTimestamp(pn, currTime);
                      }
                      tablet.addValue(schemaGeneratorPairs.get(sn).left.getMeasurementId(), pn,
                          sequenceGenerator.gen(pn));
                    }
                  }

                  tablet.rowSize = chunkPointNum;
                  for (int dn = 0; dn < deviceNum; dn++) {
                    tablet.deviceId = "d" + dn;
                    writer.write(tablet);
                  }
                  tablet.deviceId = "d2";
                  // writer.writeAligned(tablet);

                  writer.flushAllChunkGroups();
                  for (int dn = 0; dn < deviceNum; dn++) {
                    tsFileResource.updateStartTime("d" + dn, chunkTimeRange * i);
                    tsFileResource.updateEndTime("d" + dn, chunkTimeRange * (i + 1));
                  }
                }

                tsFileResource.close();
                synchronized (tsFileResources) {
                  tsFileResources.add(tsFileResource);
                }
              } catch (IOException | WriteProcessException e) {
                throw new RuntimeException(e);
              }
            });
    tsFileResources.sort(Comparator.comparingLong(TsFileResource::getVersion));
  }

  public static String getTestTsFilePath(
      String logicalStorageGroupName,
      long VirtualStorageGroupId,
      long TimePartitionId,
      long tsFileVersion) {
    String filePath =
        String.format(
            TEST_TSFILE_PATH, logicalStorageGroupName, VirtualStorageGroupId, TimePartitionId);
    return TsFileGeneratorUtils.getTsFilePath(filePath, tsFileVersion);
  }
}
