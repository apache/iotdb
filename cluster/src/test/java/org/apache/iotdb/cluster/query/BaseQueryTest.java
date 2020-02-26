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

package org.apache.iotdb.cluster.query;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestManagedSeriesReader;
import org.apache.iotdb.cluster.common.TestMetaClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;

/**
 * allNodes: node0, node1... node9
 * localNode: node0
 * pathList: root.sg0.s0, root.sg0.s1... root.sg0.s9 (all double type)
 */
public class BaseQueryTest {

  MetaGroupMember localMetaGroupMember;
  Map<Node, DataGroupMember> dataGroupMemberMap;
  Map<Node, MetaGroupMember> metaGroupMemberMap;
  List<Path> pathList;
  List<TSDataType> dataTypes;
  PlanExecutor planExecutor;
  PartitionTable partitionTable;
  List<Node> allNodes;

  @Before
  public void setUp() throws MetadataException, QueryProcessException {
    allNodes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      allNodes.add(TestUtils.getNode(i));
    }

    dataGroupMemberMap = new HashMap<>();
    metaGroupMemberMap = new HashMap<>();

    partitionTable = new SlotPartitionTable(allNodes, TestUtils.getNode(0));
    localMetaGroupMember = getMetaGroupMember(TestUtils.getNode(0));

    pathList = new ArrayList<>();
    dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      pathList.add(new Path(TestUtils.getTestSeries(0, i)));
      dataTypes.add(TSDataType.DOUBLE);
    }

    planExecutor = new PlanExecutor();

    MManager.getInstance().init();
    for (int i = 0; i < 10; i++) {
      try {
        MManager.getInstance().setStorageGroup(TestUtils.getTestSg(i));
        MeasurementSchema schema = TestUtils.getTestSchema(0, i);
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType(),
            schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (PathAlreadyExistException e) {
        // ignore
      }
    }
    QueryCoordinator.getINSTANCE().setMetaGroupMember(localMetaGroupMember);
  }

  private DataGroupMember getDataGroupMember(Node node) {
    return dataGroupMemberMap.computeIfAbsent(node, this::newDataGroupMember);
  }

  private DataGroupMember newDataGroupMember(Node node) {
    DataGroupMember newMember = new TestDataGroupMember(node, partitionTable.getHeaderGroup(node)) {
      @Override
      public boolean syncLeader() {
        return true;
      }
    };
    newMember.setThisNode(node);
    newMember.setMetaGroupMember(localMetaGroupMember);
    return newMember;
  }

  private MetaGroupMember getMetaGroupMember(Node node) {
    return metaGroupMemberMap.computeIfAbsent(node, this::newMetaGroupMember);
  }

  private MetaGroupMember newMetaGroupMember(Node node) {
    MetaGroupMember ret = new TestMetaGroupMember() {
      @Override
      public TSDataType getSeriesType(String pathStr) {
        for (int i = 0; i < pathList.size(); i++) {
          if (pathList.get(i).getFullPath().equals(pathStr)) {
            return dataTypes.get(i);
          }
        }
        return null;
      }

      @Override
      public ManagedSeriesReader getSeriesReader(Path path, TSDataType dataType, Filter timeFilter,
          Filter valueFilter, QueryContext context) {
        int pathIndex = pathList.indexOf(path);
        if (pathIndex == -1) {
          return null;
        }
        return new TestManagedSeriesReader(TestUtils.genBatchData(dataTypes.get(pathIndex), 0,
            100));
      }

      @Override
      public IReaderByTimestamp getReaderByTimestamp(Path path, TSDataType dataType,
          QueryContext context) {
        for (int i = 0; i < pathList.size(); i++) {
          if (pathList.get(i).equals(path)) {
            return new TestManagedSeriesReader(TestUtils.genBatchData(dataTypes.get(i), 0,
                100));
          }
        }
        return null;
      }

      @Override
      public List<String> getMatchedPaths(String pathPattern) throws MetadataException {
        return MManager.getInstance().getAllTimeseriesName(pathPattern);
      }

      @Override
      protected DataGroupMember getLocalDataMember(Node header, AsyncMethodCallback resultHandler,
          Object request) {
        return getDataGroupMember(getThisNode());
      }

      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestMetaClient(null, null, node, null) {
            @Override
            public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
              new Thread(() -> resultHandler.onComplete(new TNodeStatus())).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }

      @Override
      public DataClient getDataClient(Node node) throws IOException {
        return new TestDataClient(node) {

          @Override
          public void fetchSingleSeries(Node header, long readerId,
              AsyncMethodCallback<ByteBuffer> resultHandler) {
            new Thread(() -> getDataGroupMember(header).fetchSingleSeries(header, readerId,
                resultHandler)).start();
          }
        };
      }
    };
    ret.setThisNode(node);
    ret.setPartitionTable(partitionTable);
    ret.setAllNodes(allNodes);
    return ret;
  }

  @After
  public void tearDown() throws IOException {
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
    QueryCoordinator.getINSTANCE().setMetaGroupMember(null);
  }

  void checkDataset(QueryDataSet dataSet, int offset, int size) throws IOException {
    for (int i = offset; i < offset + size; i++) {
      assertTrue(dataSet.hasNext());
      RowRecord record = dataSet.next();
      assertEquals(i, record.getTimestamp());
      assertEquals(10, record.getFields().size());
      for (int j = 0; j < 10; j++) {
        assertEquals(i * 1.0, record.getFields().get(j).getDoubleV(), 0.00001);
      }
    }
    assertFalse(dataSet.hasNext());
  }
}
