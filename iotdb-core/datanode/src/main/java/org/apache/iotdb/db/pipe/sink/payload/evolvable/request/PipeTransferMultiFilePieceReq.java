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
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferMultiFilePieceReq extends PipeTransferFilePieceReq {

  private PipeTransferMultiFilePieceReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_MULTI_FILE_PIECE;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferMultiFilePieceReq toTPipeTransferReq(
      final String fileName, final long startWritingOffset, final byte[] filePiece)
      throws IOException {
    return (PipeTransferMultiFilePieceReq)
        new PipeTransferMultiFilePieceReq()
            .convertToTPipeTransferReq(fileName, startWritingOffset, filePiece);
  }

  public static PipeTransferMultiFilePieceReq toTPipeTransferReq(
      final String fileName, final ByteBuffer filePiece, final int transferSize)
      throws IOException {
    return (PipeTransferMultiFilePieceReq)
        new PipeTransferMultiFilePieceReq()
            .convertToTPipeTransferReq(fileName, filePiece, transferSize);
  }

  public static PipeTransferMultiFilePieceReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    return (PipeTransferMultiFilePieceReq)
        new PipeTransferMultiFilePieceReq().translateFromTPipeTransferReq(transferReq);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final String fileName, final long startWritingOffset, final byte[] filePiece)
      throws IOException {
    return new PipeTransferMultiFilePieceReq()
        .convertToTPipeTransferBytes(fileName, startWritingOffset, filePiece);
  }

  public static byte[] toTPipeTransferBytes(
      final String fileName, final ByteBuffer filePiece, final int transferSize)
      throws IOException {
    return new PipeTransferMultiFilePieceReq()
        .convertToTPipeTransferBytes(fileName, filePiece, transferSize);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeTransferMultiFilePieceReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
