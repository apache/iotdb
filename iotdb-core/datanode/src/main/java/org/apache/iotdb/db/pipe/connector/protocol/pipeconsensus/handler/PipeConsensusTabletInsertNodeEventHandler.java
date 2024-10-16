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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.handler;

import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.consensus.metric.PipeConsensusConnectorMetrics;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;

import org.apache.thrift.TException;

public class PipeConsensusTabletInsertNodeEventHandler
    extends PipeConsensusTabletInsertionEventHandler<TPipeConsensusTransferResp> {

  public PipeConsensusTabletInsertNodeEventHandler(
      PipeInsertNodeTabletInsertionEvent event,
      TPipeConsensusTransferReq req,
      PipeConsensusAsyncConnector connector,
      PipeConsensusConnectorMetrics metric) {
    super(event, req, connector, metric);
  }

  @Override
  protected void doTransfer(AsyncPipeConsensusServiceClient client, TPipeConsensusTransferReq req)
      throws TException {
    client.pipeConsensusTransfer(req, this);
  }
}
