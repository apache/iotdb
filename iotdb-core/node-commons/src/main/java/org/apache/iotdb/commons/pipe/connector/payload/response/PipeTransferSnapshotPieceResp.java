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

package org.apache.iotdb.commons.pipe.connector.payload.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeTransferSnapshotPieceResp extends TPipeTransferResp {

  public static final long ERROR_END_OFFSET = -1;

  private long endWritingOffset;

  private PipeTransferSnapshotPieceResp() {}

  public long getEndWritingOffset() {
    return endWritingOffset;
  }

  public static PipeTransferSnapshotPieceResp toTPipeTransferResp(
      TSStatus status, long endWritingOffset) throws IOException {
    final PipeTransferSnapshotPieceResp snapshotPieceResp = new PipeTransferSnapshotPieceResp();

    snapshotPieceResp.status = status;

    snapshotPieceResp.endWritingOffset = endWritingOffset;
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(endWritingOffset, outputStream);
      snapshotPieceResp.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return snapshotPieceResp;
  }

  public static PipeTransferSnapshotPieceResp toTPipeTransferResp(TSStatus status) {
    final PipeTransferSnapshotPieceResp snapshotPieceResp = new PipeTransferSnapshotPieceResp();

    snapshotPieceResp.status = status;

    return snapshotPieceResp;
  }

  public static PipeTransferSnapshotPieceResp fromTPipeTransferResp(
      TPipeTransferResp transferResp) {
    final PipeTransferSnapshotPieceResp snapshotPieceResp = new PipeTransferSnapshotPieceResp();

    snapshotPieceResp.status = transferResp.status;

    if (transferResp.isSetBody()) {
      snapshotPieceResp.endWritingOffset = ReadWriteIOUtils.readLong(transferResp.body);
      snapshotPieceResp.body = transferResp.body;
    }

    return snapshotPieceResp;
  }
}
