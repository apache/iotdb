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

package org.apache.iotdb.cluster.common;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.async.AsyncMethodCallback;

public class TestDataClient extends DataClient {

  private PlanExecutor planExecutor;
  private Map<Node, DataGroupMember> dataGroupMemberMap;

  public TestDataClient(Node node,
      Map<Node, DataGroupMember> dataGroupMemberMap)
      throws IOException {
    super(null, null, node, null);
    this.dataGroupMemberMap = dataGroupMemberMap;
    try {
      this.planExecutor = new PlanExecutor();
    } catch (QueryProcessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void fetchSingleSeries(Node header, long readerId,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(header).fetchSingleSeries(header, readerId,
        resultHandler)).start();
  }

  @Override
  public void getAggrResult(GetAggrResultRequest request,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(request.getHeader()).getAggrResult(request,
        resultHandler)).start();
  }

  @Override
  public void querySingleSeries(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(request.getHeader()).querySingleSeries(request,
        resultHandler)).start();
  }

  @Override
  public void fetchSingleSeriesByTimestamp(Node header, long readerId, ByteBuffer timeBytes,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(header).fetchSingleSeriesByTimestamp(header,
        readerId, timeBytes, resultHandler)).start();
  }

  @Override
  public void getAllPaths(Node header, String path,
      AsyncMethodCallback<List<String>> resultHandler) {
    new Thread(() -> {
      try {
        resultHandler.onComplete(MManager.getInstance().getAllTimeseriesName(path));
      } catch (MetadataException e) {
        resultHandler.onError(e);
      }
    }).start();
  }

  @Override
  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    new Thread(() -> {
      try {
        PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
        planExecutor.processNonQuery(plan);
        resultHandler.onComplete(StatusUtils.OK);
      } catch (IOException | QueryProcessException e) {
        resultHandler.onError(e);
      }
    }).start();
  }

  @Override
  public void readFile(String filePath, long offset, int length, Node header,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    new Thread(() -> {
      if (offset == 0) {
        resultHandler.onComplete(
            ByteBuffer.wrap((filePath + "@" + offset + "#" + length).getBytes()));
      } else {
        resultHandler.onComplete(ByteBuffer.allocate(0));
      }
    }).start();
  }

  @Override
  public void startElection(ElectionRequest request,
      AsyncMethodCallback<Long> resultHandler) {
  }

  @Override
  public void appendEntry(AppendEntryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    new Thread(() -> resultHandler.onComplete(Response.RESPONSE_AGREE)).start();
  }

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    new Thread(() -> {
      PullSchemaResp resp;
      try {
        resp = new PullSchemaResp();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        dataOutputStream.writeInt(10);
        for (int i = 0; i < 10; i++) {
          TestUtils.getTestSchema(0, i).serializeTo(dataOutputStream);
        }
        resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
        resultHandler.onComplete(resp);
      } catch (IOException e) {
        resultHandler.onError(e);
      }
    }).start();
  }

  @Override
  public void querySingleSeriesByTimestamp(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(request.getHeader()).querySingleSeriesByTimestamp(request,
        resultHandler)).start();
  }

  @Override
  public void getGroupByExecutor(GroupByRequest request, AsyncMethodCallback<Long> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(request.getHeader()).getGroupByExecutor(request,
        resultHandler)).start();
  }

  @Override
  public void getGroupByResult(Node header, long executorId, long startTime, long endTime,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    new Thread(() -> dataGroupMemberMap.get(header).getGroupByResult(header, executorId,
        startTime, endTime, resultHandler)).start();
  }


}
