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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    int nextNodePos = (this.curPos + 1) % this.nodes.size();
    while (true) {
      Node node = nodes.get(nextNodePos);
      logger.debug("querying {} from {} of {}", request.path, node, partitionGroup.getHeader());
      try {

        DataClient client = this.metaGroupMember.getDataClient(node);
        Long newReaderId = applyForReaderId(client, byTimestamp, timestamp);

        if (newReaderId != null) {
          logger.debug("get a readerId {} for {} from {}", newReaderId, request.path, node);
          if (newReaderId != -1) {
            // register the node so the remote resources can be released
            context.registerRemoteNode(node, partitionGroup.getHeader());
            this.readerId = newReaderId;
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
      } catch (TException | IOException e) {
        logger.error("Cannot query {} from {}", this.request.path, node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
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

  private Long applyForReaderId(DataClient client, boolean byTimestamp, long timestamp)
      throws TException, InterruptedException {
    Long newReaderId;
    if (byTimestamp) {
      newReaderId = SyncClientAdaptor.querySingleSeriesByTimestamp(client, request);
    } else {
      newReaderId = SyncClientAdaptor.querySingleSeries(client, request, timestamp);
    }
    return newReaderId;
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

  @Override
  public String toString() {
    return "DataSourceInfo{" +
        "readerId=" + readerId +
        ", curSource=" + curSource +
        ", partitionGroup=" + partitionGroup +
        ", request=" + request +
        '}';
  }

  /**
   * Check if there is still any available client and there is still any left data.
   * @return true if there is an available client and data to read, false all data has been read.
   * @throws IOException if all clients are unavailable.
   */
  boolean checkCurClient() throws IOException {
    if (getCurClient() == null) {
      if (!isNoData()) {
        throw new IOException("no available client.");
      } else {
        // no data
        return false;
      }
    }
    return true;
  }

  boolean switchNode(boolean byTimestamp, long timeOffset) throws IOException {
    DataClient newClient = nextDataClient(byTimestamp, timeOffset);
    logger.info("Client failed, changed to {}", newClient);
    if (newClient == null) {
      if (!isNoData()) {
        throw new IOException("no available client.");
      } else {
        // no data
        return false;
      }
    }
    return true;
  }
}
