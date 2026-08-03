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

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import java.nio.ByteBuffer;

/** Typed response payload for an Object Tablet batch sequence reset. */
public class PipeTransferTabletObjectBatchResp {

  private final int expectedSequenceId;

  private PipeTransferTabletObjectBatchResp(final int expectedSequenceId) {
    this.expectedSequenceId = expectedSequenceId;
  }

  public static TPipeTransferResp toSequenceResetTPipeTransferResp(final int expectedSequenceId) {
    if (expectedSequenceId < 0) {
      throw new IllegalArgumentException();
    }
    return new TPipeTransferResp(
            RpcUtils.getStatus(TSStatusCode.PIPE_TRANSFER_TABLET_OBJECT_BATCH_SEQUENCE_RESET))
        .setBody(ByteBuffer.allocate(Integer.BYTES).putInt(expectedSequenceId).flip());
  }

  public static PipeTransferTabletObjectBatchResp fromTPipeTransferResp(
      final TPipeTransferResp transferResp) {
    final ByteBuffer body = transferResp == null ? null : transferResp.bufferForBody();
    if (body == null || body.remaining() != Integer.BYTES) {
      throw new PipeException(
          DataNodePipeMessages.EXCEPTION_OBJECT_TABLET_BATCH_INVALID_SEQUENCE_RESET_BODY_FE990B3B);
    }
    final int expectedSequenceId = body.getInt(body.position());
    if (expectedSequenceId < 0) {
      throw new PipeException(
          DataNodePipeMessages.EXCEPTION_OBJECT_TABLET_BATCH_INVALID_SEQUENCE_RESET_BODY_FE990B3B);
    }
    return new PipeTransferTabletObjectBatchResp(expectedSequenceId);
  }

  public int getExpectedSequenceId() {
    return expectedSequenceId;
  }
}
