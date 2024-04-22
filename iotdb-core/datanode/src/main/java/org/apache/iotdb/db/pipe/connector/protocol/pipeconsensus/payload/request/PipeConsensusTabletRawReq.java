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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request;

import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeConsensusTabletRawReq extends TPipeConsensusTransferReq {

  private transient Tablet tablet;
  private transient boolean isAligned;

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeConsensusTabletRawReq toTPipeConsensusTransferRawReq(
      Tablet tablet, boolean isAligned) {
    final PipeConsensusTabletRawReq tabletReq = new PipeConsensusTabletRawReq();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;

    return tabletReq;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeConsensusTabletRawReq toTPipeConsensusTransferReq(
      Tablet tablet, boolean isAligned) throws IOException {
    final PipeConsensusTabletRawReq tabletReq = new PipeConsensusTabletRawReq();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;

    tabletReq.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    tabletReq.type = PipeConsensusRequestType.TRANSFER_TABLET_RAW.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      tabletReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return tabletReq;
  }

  public static PipeConsensusTabletRawReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    final PipeConsensusTabletRawReq tabletReq = new PipeConsensusTabletRawReq();

    tabletReq.tablet = Tablet.deserialize(transferReq.body);
    tabletReq.isAligned = ReadWriteIOUtils.readBool(transferReq.body);

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;
    tabletReq.body = transferReq.body;

    return tabletReq;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeConsensusTabletRawReq that = (PipeConsensusTabletRawReq) obj;
    return Objects.equals(tablet, that.tablet)
        && isAligned == that.isAligned
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tablet, isAligned, version, type, body);
  }
}
