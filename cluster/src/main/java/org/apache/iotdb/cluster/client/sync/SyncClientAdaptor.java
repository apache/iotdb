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

package org.apache.iotdb.cluster.client.sync;

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

import java.util.concurrent.atomic.AtomicReference;
import javax.xml.crypto.Data;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.client.async.MetaClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.thrift.TException;

/**
 * SyncClientAdaptor convert the async of AsyncClient method call to a sync one.
 */
@SuppressWarnings("java:S2274") // enable timeout
public class SyncClientAdaptor {

  private SyncClientAdaptor() {
    // static class
  }

  public static Long removeNode(MetaClient metaClient, Node nodeToRemove)
      throws TException, InterruptedException {
    AtomicReference<Long> responseRef = new AtomicReference<>();
    GenericHandler handler = new GenericHandler(metaClient.getNode(), responseRef);
    synchronized (responseRef) {
      metaClient.removeNode(nodeToRemove, handler);
      responseRef.wait(RaftServer.connectionTimeoutInMS);
    }
    return responseRef.get();
  }

  public static Boolean matchTerm(AsyncClient client, Node target, long prevLogIndex,
      long prevLogTerm, Node header) throws TException, InterruptedException {
    AtomicReference<Boolean> resultRef = new AtomicReference<>(false);
    GenericHandler matchTermHandler = new GenericHandler(target, resultRef);

    synchronized (resultRef) {
      client.matchTerm(prevLogIndex, prevLogTerm, header, matchTermHandler);
      resultRef.wait(RaftServer.connectionTimeoutInMS);
    }
    return resultRef.get();
  }

  public static Long querySingleSeriesByTimestamp(DataClient client, SingleSeriesQueryRequest request)
      throws TException, InterruptedException {
    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(client.getNode(), result);
    synchronized (result) {
      client.querySingleSeriesByTimestamp(request, handler);
      result.wait(connectionTimeoutInMS);
    }
    return result.get();
  }

  public static Long querySingleSeries(DataClient client, SingleSeriesQueryRequest request,
      long timeOffset) throws TException, InterruptedException {
    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(client.getNode(), result);
    Filter newFilter;
    // add timestamp to as a timeFilter to skip the data which has been read
    if (request.isSetTimeFilterBytes()) {
      Filter timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
      newFilter = new AndFilter(timeFilter, TimeFilter.gt(timeOffset));
    } else {
      newFilter = TimeFilter.gt(timeOffset);
    }
    request.setTimeFilterBytes(SerializeUtils.serializeFilter(newFilter));

    synchronized (result) {
      client.querySingleSeries(request, handler);
      result.wait(connectionTimeoutInMS);
    }
    return result.get();
  }
}
