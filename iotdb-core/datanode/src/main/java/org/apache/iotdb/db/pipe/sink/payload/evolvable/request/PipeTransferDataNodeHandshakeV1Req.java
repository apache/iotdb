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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferDataNodeHandshakeV1Req extends PipeTransferHandshakeV1Req {

  private PipeTransferDataNodeHandshakeV1Req() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.HANDSHAKE_DATANODE_V1;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferDataNodeHandshakeV1Req toTPipeTransferReq(
      final String timestampPrecision) throws IOException {
    return (PipeTransferDataNodeHandshakeV1Req)
        new PipeTransferDataNodeHandshakeV1Req().convertToTPipeTransferReq(timestampPrecision);
  }

  public static PipeTransferDataNodeHandshakeV1Req fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    return (PipeTransferDataNodeHandshakeV1Req)
        new PipeTransferDataNodeHandshakeV1Req().translateFromTPipeTransferReq(transferReq);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final String timestampPrecision) throws IOException {
    return new PipeTransferDataNodeHandshakeV1Req()
        .convertToTransferHandshakeBytes(timestampPrecision);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeTransferDataNodeHandshakeV1Req && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
