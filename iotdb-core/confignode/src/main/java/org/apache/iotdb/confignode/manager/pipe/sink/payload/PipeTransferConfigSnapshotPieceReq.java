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
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferConfigSnapshotPieceReq extends PipeTransferFilePieceReq {

  private PipeTransferConfigSnapshotPieceReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigSnapshotPieceReq toTPipeTransferReq(
      String fileName, long startWritingOffset, byte[] filePiece) throws IOException {
    return (PipeTransferConfigSnapshotPieceReq)
        new PipeTransferConfigSnapshotPieceReq()
            .convertToTPipeTransferReq(fileName, startWritingOffset, filePiece);
  }

  public static PipeTransferConfigSnapshotPieceReq fromTPipeTransferReq(
      TPipeTransferReq transferReq) {
    return (PipeTransferConfigSnapshotPieceReq)
        new PipeTransferConfigSnapshotPieceReq().translateFromTPipeTransferReq(transferReq);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      String fileName, long startWritingOffset, byte[] filePiece) throws IOException {
    return new PipeTransferConfigSnapshotPieceReq()
        .convertToTPipeTransferBytes(fileName, startWritingOffset, filePiece);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferConfigSnapshotPieceReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
