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

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** An ordered, atomic-at-Pipe-progress batch for one Object-bearing Tablet insertion event. */
public class PipeTransferTabletObjectBatchReq extends TPipeTransferReq {

  private static final int SERIALIZED_BATCH_HEADER_SIZE =
      Integer.BYTES + Long.BYTES + Integer.BYTES + 1;
  private static final int SERIALIZED_TABLET_LENGTH_SIZE = Integer.BYTES;

  private transient List<PipeTransferTabletRawReqV2> tabletRequests;
  private transient long batchId;
  private transient int sequenceId;
  private transient boolean last;

  private PipeTransferTabletObjectBatchReq() {
    // utility constructor
  }

  public List<PipeTransferTabletRawReqV2> getTabletRequests() {
    return tabletRequests;
  }

  public static int getSerializedBatchHeaderSize() {
    return SERIALIZED_BATCH_HEADER_SIZE;
  }

  public static int getSerializedTabletSize(final byte[] serializedTablet) {
    return SERIALIZED_TABLET_LENGTH_SIZE + serializedTablet.length;
  }

  public static int getSerializedTabletLengthSize() {
    return SERIALIZED_TABLET_LENGTH_SIZE;
  }

  public static PipeTransferTabletObjectBatchReq toTPipeTransferReq(
      final long batchId,
      final int sequenceId,
      final boolean last,
      final List<Tablet> tablets,
      final List<Boolean> alignments,
      final String databaseName)
      throws IOException {
    if (tablets.size() != alignments.size()) {
      throw new IllegalArgumentException();
    }
    final List<byte[]> serializedTablets = new ArrayList<>(tablets.size());
    for (int i = 0; i < tablets.size(); i++) {
      serializedTablets.add(
          PipeTransferTabletRawReqV2.toTPipeTransferReq(
                  tablets.get(i), alignments.get(i), databaseName)
              .getBody());
    }
    return toTPipeTransferReq(batchId, sequenceId, last, serializedTablets);
  }

  public static PipeTransferTabletObjectBatchReq toTPipeTransferReq(
      final long batchId,
      final int sequenceId,
      final boolean last,
      final List<byte[]> serializedTablets)
      throws IOException {
    final PipeTransferTabletObjectBatchReq req = new PipeTransferTabletObjectBatchReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_OBJECT_BATCH.getType();
    try (final PublicBAOS output = new PublicBAOS(calculateSerializedSize(serializedTablets));
        final DataOutputStream dataOutput = new DataOutputStream(output)) {
      ReadWriteIOUtils.write(serializedTablets.size(), dataOutput);
      ReadWriteIOUtils.write(batchId, dataOutput);
      ReadWriteIOUtils.write(sequenceId, dataOutput);
      ReadWriteIOUtils.write(last, dataOutput);
      for (final byte[] serializedTablet : serializedTablets) {
        ReadWriteIOUtils.write(serializedTablet.length, dataOutput);
        dataOutput.write(serializedTablet);
      }
      req.body = ByteBuffer.wrap(output.getBuf(), 0, output.size());
    }
    return req;
  }

  static int calculateSerializedSize(final List<byte[]> serializedTablets) {
    int size = Integer.BYTES + Long.BYTES + Integer.BYTES + 1;
    for (final byte[] serializedTablet : serializedTablets) {
      size += Integer.BYTES + serializedTablet.length;
    }
    return size;
  }

  public static PipeTransferTabletObjectBatchReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferTabletObjectBatchReq req = new PipeTransferTabletObjectBatchReq();
    final int count = ReadWriteIOUtils.readInt(transferReq.body);
    req.batchId = ReadWriteIOUtils.readLong(transferReq.body);
    req.sequenceId = ReadWriteIOUtils.readInt(transferReq.body);
    req.last = ReadWriteIOUtils.readBool(transferReq.body);
    req.tabletRequests = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final int length = ReadWriteIOUtils.readInt(transferReq.body);
      final ByteBuffer serializedTablet = transferReq.body.slice();
      serializedTablet.limit(length);
      req.tabletRequests.add(PipeTransferTabletRawReqV2.toTPipeTransferRawReq(serializedTablet));
      transferReq.body.position(transferReq.body.position() + length);
    }
    req.version = transferReq.version;
    req.type = transferReq.type;
    req.body = transferReq.body;
    return req;
  }

  public long getBatchId() {
    return batchId;
  }

  public int getSequenceId() {
    return sequenceId;
  }

  public boolean isLast() {
    return last;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTransferTabletObjectBatchReq that = (PipeTransferTabletObjectBatchReq) obj;
    return batchId == that.batchId
        && sequenceId == that.sequenceId
        && last == that.last
        && version == that.version
        && type == that.type
        && Objects.equals(tabletRequests, that.tabletRequests)
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tabletRequests, batchId, sequenceId, last, version, type, body);
  }
}
