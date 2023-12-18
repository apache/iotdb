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

package org.apache.iotdb.confignode.manager.pipe.connector.payload.request;

import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeTransferFilePieceReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferConfigSnapShotPieceReq extends PipeTransferFilePieceReq {
  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigSnapShotPieceReq toTPipeTransferReq(
      String fileName, long startWritingOffset, byte[] filePiece) throws IOException {
    return (PipeTransferConfigSnapShotPieceReq)
        new PipeTransferConfigSnapShotPieceReq()
            .convertToTPipeTransferReq(fileName, startWritingOffset, filePiece);
  }

  public static PipeTransferConfigSnapShotPieceReq fromTPipeTransferReq(
      TPipeTransferReq transferReq) {
    return (PipeTransferConfigSnapShotPieceReq)
        new PipeTransferConfigSnapShotPieceReq().translateFromTPipeTransferReq(transferReq);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      String fileName, long startWritingOffset, byte[] filePiece) throws IOException {
    return new PipeTransferConfigSnapShotPieceReq()
        .convertToTPipeTransferBytes(fileName, startWritingOffset, filePiece);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferConfigSnapShotPieceReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
