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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.commons.service.metric.MetricService;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

<<<<<<<< HEAD:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/exchange/MPPDataExchangeServiceThriftHandler.java
import java.util.concurrent.atomic.AtomicLong;

public class MPPDataExchangeServiceThriftHandler implements TServerEventHandler {
  private AtomicLong thriftConnectionNumber = new AtomicLong(0);

  public MPPDataExchangeServiceThriftHandler() {
    MetricService.getInstance()
        .addMetricSet(new MppDataExchangeServiceThriftHandlerMetrics(thriftConnectionNumber));
========
public class IoTConsensusRpcServiceHandler implements TServerEventHandler {

  private final IoTConsensusRpcServiceProcessor processor;

  public IoTConsensusRpcServiceHandler(IoTConsensusRpcServiceProcessor processor) {
    this.processor = processor;
>>>>>>>> dc77bbc259c93fa20bfd64b02265457f29662454:consensus/src/main/java/org/apache/iotdb/consensus/iot/service/IoTConsensusRpcServiceHandler.java
  }

  @Override
  public void preServe() {
<<<<<<<< HEAD:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/exchange/MPPDataExchangeServiceThriftHandler.java
    // empty override
========
    // Empty method, since it is not needed now
>>>>>>>> dc77bbc259c93fa20bfd64b02265457f29662454:consensus/src/main/java/org/apache/iotdb/consensus/iot/service/IoTConsensusRpcServiceHandler.java
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    thriftConnectionNumber.incrementAndGet();
    return null;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    thriftConnectionNumber.decrementAndGet();
  }

  @Override
  public void processContext(
<<<<<<<< HEAD:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/exchange/MPPDataExchangeServiceThriftHandler.java
      ServerContext serverContext, TTransport inputTransport, TTransport outputTransport1) {
    // empty override
========
      ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
    // Empty method, since it is not needed now
>>>>>>>> dc77bbc259c93fa20bfd64b02265457f29662454:consensus/src/main/java/org/apache/iotdb/consensus/iot/service/IoTConsensusRpcServiceHandler.java
  }
}
