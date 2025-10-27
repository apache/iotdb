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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTreeModelTabletEventSorter;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent.isTabletEmpty;

public class PipeTransferTabletRawReq extends TPipeTransferReq {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTabletRawReq.class);

  protected transient Tablet tablet;
  protected transient boolean isAligned;

  public Tablet getTablet() {
    return tablet;
  }

  public boolean getIsAligned() {
    return isAligned;
  }

  public InsertTabletStatement constructStatement() {
    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    try {
      if (isTabletEmpty(tablet)) {
        // Empty statement, will be filtered after construction
        return new InsertTabletStatement();
      }

      return new InsertTabletStatement(tablet, isAligned, null);
    } catch (final MetadataException e) {
      LOGGER.warn("Generate Statement from tablet {} error.", tablet, e);
      return null;
    }
  }

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeTransferTabletRawReq toTPipeTransferRawReq(
      final Tablet tablet, final boolean isAligned) {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;

    return tabletReq;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletRawReq toTPipeTransferReq(
      final Tablet tablet, final boolean isAligned) throws IOException {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;

    tabletReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    tabletReq.type = PipeRequestType.TRANSFER_TABLET_RAW.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      tabletReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return tabletReq;
  }

  public static PipeTransferTabletRawReq fromTPipeTransferReq(final TPipeTransferReq transferReq) {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.tablet = Tablet.deserialize(transferReq.body);
    tabletReq.isAligned = ReadWriteIOUtils.readBool(transferReq.body);

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;

    return tabletReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final Tablet tablet, final boolean isAligned)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_RAW.getType(), outputStream);
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTransferTabletRawReq that = (PipeTransferTabletRawReq) obj;
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
