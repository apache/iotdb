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

package org.apache.iotdb.confignode.manager.pipe.sink.payload;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferConfigNodeHandshakeV1Req extends PipeTransferHandshakeV1Req {

  private PipeTransferConfigNodeHandshakeV1Req() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.HANDSHAKE_CONFIGNODE_V1;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigNodeHandshakeV1Req toTPipeTransferReq(String timestampPrecision)
      throws IOException {
    return (PipeTransferConfigNodeHandshakeV1Req)
        new PipeTransferConfigNodeHandshakeV1Req().convertToTPipeTransferReq(timestampPrecision);
  }

  public static PipeTransferConfigNodeHandshakeV1Req fromTPipeTransferReq(
      TPipeTransferReq transferReq) {
    return (PipeTransferConfigNodeHandshakeV1Req)
        new PipeTransferConfigNodeHandshakeV1Req().translateFromTPipeTransferReq(transferReq);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(String timestampPrecision) throws IOException {
    return new PipeTransferConfigNodeHandshakeV1Req()
        .convertToTransferHandshakeBytes(timestampPrecision);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferConfigNodeHandshakeV1Req && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
