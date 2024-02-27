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

package org.apache.iotdb.db.pipe.connector.protocol.airgap;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBAirGapCommonConnector;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.io.IOException;
import java.net.Socket;

public abstract class IoTDBAirGapDataNodeConnector extends IoTDBAirGapCommonConnector {
  @Override
  protected byte[] getHandShakeBytes() throws IOException {
    return PipeTransferDataNodeHandshakeV1Req.toTPipeTransferBytes(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  protected void doTransfer(
      Socket socket, PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    if (!send(
        socket,
        PipeTransferPlanNodeReq.toTPipeTransferBytes(
            pipeSchemaRegionWritePlanEvent.getPlanNode()))) {
      throw new PipeException(
          String.format(
              "Transfer PipeWriteSchemaPlanEvent %s error. Socket: %s.",
              pipeSchemaRegionWritePlanEvent, socket));
    }
  }
}
