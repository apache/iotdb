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

package org.apache.iotdb.db.pipe.connector.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;

import java.util.List;

public abstract class IoTDBThriftClientManager {

  protected final List<TEndPoint> endPointList;

  protected long currentClientIndex = 0;

  protected final boolean useLeaderCache;
  protected static final LeaderCacheManager leaderCacheManager = new LeaderCacheManager();

  protected PipeRequestType receiverHandshakeType = PipeRequestType.HANDSHAKE_V2;

  protected IoTDBThriftClientManager(List<TEndPoint> endPointList, boolean useLeaderCache) {
    this.endPointList = endPointList;
    this.useLeaderCache = useLeaderCache;
  }

  public PipeRequestType getReceiverHandshakeType() {
    return receiverHandshakeType;
  }
}
