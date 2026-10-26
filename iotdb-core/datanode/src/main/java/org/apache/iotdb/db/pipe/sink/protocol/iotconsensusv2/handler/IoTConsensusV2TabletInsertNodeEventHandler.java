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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler;

import org.apache.iotdb.commons.client.async.AsyncIoTConsensusV2ServiceClient;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.IoTConsensusV2AsyncSink;

import org.apache.thrift.TException;

public class IoTConsensusV2TabletInsertNodeEventHandler
    extends IoTConsensusV2TabletInsertionEventHandler<TIoTConsensusV2TransferResp> {

  public IoTConsensusV2TabletInsertNodeEventHandler(
      PipeInsertNodeTabletInsertionEvent event,
      TIoTConsensusV2TransferReq req,
      IoTConsensusV2AsyncSink connector,
      IoTConsensusV2SinkMetrics metric) {
    super(event, req, connector, metric);
  }

  @Override
  protected void doTransfer(AsyncIoTConsensusV2ServiceClient client, TIoTConsensusV2TransferReq req)
      throws TException {
    client.iotConsensusV2Transfer(req, this);
  }
}
