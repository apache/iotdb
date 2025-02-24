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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.thrift.TException;

import java.util.concurrent.atomic.AtomicInteger;

public class PipeTransferTabletRawEventHandler extends PipeTransferTabletInsertionEventHandler {

  public PipeTransferTabletRawEventHandler(
      final PipeRawTabletInsertionEvent event,
      final TPipeTransferReq req,
      final IoTDBDataRegionAsyncConnector connector,
      final AtomicInteger eventsReferenceCount) {
    super(event, req, connector, eventsReferenceCount);
  }

  @Override
  protected void doTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    client.pipeTransfer(req, this);
  }

  @Override
  protected void updateLeaderCache(final TSStatus status) {
    connector.updateLeaderCache(
        ((PipeRawTabletInsertionEvent) event).getDeviceId(), status.getRedirectNode());
  }
}
