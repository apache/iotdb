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
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.util.Map;

public class PipeTransferConfigNodeHandshakeV2Req extends PipeTransferHandshakeV2Req {

  private PipeTransferConfigNodeHandshakeV2Req() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.HANDSHAKE_CONFIGNODE_V2;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigNodeHandshakeV2Req toTPipeTransferReq(Map<String, String> params)
      throws IOException {
    return (PipeTransferConfigNodeHandshakeV2Req)
        new PipeTransferConfigNodeHandshakeV2Req().convertToTPipeTransferReq(params);
  }

  public static PipeTransferConfigNodeHandshakeV2Req fromTPipeTransferReq(
      TPipeTransferReq transferReq) {
    return (PipeTransferConfigNodeHandshakeV2Req)
        new PipeTransferConfigNodeHandshakeV2Req().translateFromTPipeTransferReq(transferReq);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(Map<String, String> params) throws IOException {
    return new PipeTransferConfigNodeHandshakeV2Req().convertToTransferHandshakeBytes(params);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferConfigNodeHandshakeV2Req && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
