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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

/**
 * provide client which could connect to all nodes of the partitionGroup.
 * Notice: methods like getter should be called only after nextDataClient() has been called
 */
public class DataSourceInfo {
  private static final Logger logger = LoggerFactory.getLogger(DataSourceInfo.class);

  private long readerId;
  private Node curSource;
  private PartitionGroup partitionGroup;
  private TSDataType dataType;
  private SingleSeriesQueryRequest request;
  private RemoteQueryContext context;
  private MetaGroupMember metaGroupMember;
  private List<Node> nodes;
  private boolean isNoData;
  private int curPos;
  private boolean noClient = false;

  public DataSourceInfo(PartitionGroup group, TSDataType dataType,
                        SingleSeriesQueryRequest request, RemoteQueryContext context,
                        MetaGroupMember metaGroupMember, List<Node> nodes) {
    this.readerId = -1;
    this.partitionGroup = group;
    this.dataType = dataType;
    this.request = request;
    this.context = context;
    this.metaGroupMember = metaGroupMember;
    this.nodes = nodes;
    this.isNoData = false;
    // set to the last node so after nextDataClient() is called it will scan from the first node
    this.curPos = nodes.size() - 1;
    this.curSource = nodes.get(curPos);
  }

  public DataClient nextDataClient(boolean byTimestamp, long timestamp) {
    if (this.nodes.isEmpty()) {
      this.isNoData = false;
      return null;
    }

    AtomicReference<Long> result = new AtomicReference<>();

    int nextNodePos = (this.curPos + 1) % this.nodes.size();
    while (true) {
      Node node = nodes.get(nextNodePos);
      logger.debug("querying {} from {} of {}", request.path, node, partitionGroup.getHeader());
      GenericHandler<Long> handler = new GenericHandler<>(node, result);
      try {
        DataClient client = this.metaGroupMember.getDataClient(node);
        synchronized (result) {
          result.set(null);
          if (byTimestamp) {
            client.querySingleSeriesByTimestamp(request, handler);
          } else {
            // add timestamp to as a timeFilter to skip the data which has been read
            Filter newFilter;
            if (request.isSetTimeFilterBytes()) {
              Filter timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
              newFilter = new AndFilter(timeFilter, TimeFilter.gt(timestamp));
            } else {
              newFilter = TimeFilter.gt(timestamp);
            }
            request.setTimeFilterBytes(SerializeUtils.serializeFilter(newFilter));
            client.querySingleSeries(request, handler);
          }

          result.wait(connectionTimeoutInMS);
        }
        Long readerId = result.get();
        if (readerId != null) {
          logger.debug("get a readerId {} for {} from {}", readerId, request.path, node);
          if (readerId != -1) {
            // register the node so the remote resources can be released
            context.registerRemoteNode(partitionGroup.getHeader(), node);
            this.readerId = readerId;
            this.curSource = node;
            this.curPos = nextNodePos;
            return client;
          } else {
            // the id being -1 means there is no satisfying data on the remote node, create an
            // empty reader to reduce further communication
            this.noClient = true;
            this.isNoData = true;
            return null;
          }
        }
      } catch (TException | InterruptedException | IOException e) {
        logger.error("Cannot query {} from {}", this.request.path, node, e);
      }
      nextNodePos = (nextNodePos + 1) % this.nodes.size();
      if (nextNodePos == this.curPos) {
        // has iterate over all nodes
        noClient = true;
        break;
      }
    }
    // all nodes are failed
    this.isNoData = false;
    return null;
  }

  public long getReaderId() {
    return this.readerId;
  }

  public TSDataType getDataType() {
    return this.dataType;
  }

  public Node getHeader() {
    return partitionGroup.getHeader();
  }

  public Node getCurrentNode() {
    return this.curSource;
  }

  public DataClient getCurClient() throws IOException {
    return noClient ? null : metaGroupMember.getDataClient(this.curSource);
  }

  public boolean isNoData() {
    return this.isNoData;
  }

  public void setMetaGroupMemberForTest(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }
}
