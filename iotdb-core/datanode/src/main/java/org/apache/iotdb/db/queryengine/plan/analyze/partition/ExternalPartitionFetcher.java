/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.analyze.partition;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.partition.PartitionCache;

import org.apache.thrift.TException;

import java.util.List;

/**
 * ExternalPartitionFetcher fetches partition info from another cluster for cross-cluster data
 * communications.
 */
public class ExternalPartitionFetcher extends BasicPartitionFetcher {

  private List<TEndPoint> externalConfigNodes;
  private ThriftClientProperty property;

  public ExternalPartitionFetcher(
      List<TEndPoint> externalConfigNodes,
      ThriftClientProperty property,
      int seriesPartitionSlotNum) {
    super(
        seriesPartitionSlotNum,
        new PartitionCache(() -> new ConfigNodeClient(externalConfigNodes, property, null)));
    this.externalConfigNodes = externalConfigNodes;
    this.property = property;
  }

  @Override
  protected ConfigNodeClient getClient() throws TException {
    return new ConfigNodeClient(externalConfigNodes, property, null);
  }
}
