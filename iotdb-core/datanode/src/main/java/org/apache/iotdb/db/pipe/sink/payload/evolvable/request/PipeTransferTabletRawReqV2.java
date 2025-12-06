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
import org.apache.iotdb.db.pipe.sink.util.TabletStatementConverter;
import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTableModelTabletEventSorter;
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

public class PipeTransferTabletRawReqV2 extends PipeTransferTabletRawReq {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTabletRawReqV2.class);

  protected transient String dataBaseName;

  public String getDataBaseName() {
    return dataBaseName;
  }

  @Override
  public InsertTabletStatement constructStatement() {
    if (statement != null) {
      if (Objects.isNull(dataBaseName)) {
        new PipeTreeModelTabletEventSorter(statement).deduplicateAndSortTimestampsIfNecessary();
      } else {
        new PipeTableModelTabletEventSorter(statement).sortByTimestampIfNecessary();
      }

      return statement;
    }

    if (Objects.isNull(dataBaseName)) {
      new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();
    } else {
      new PipeTableModelTabletEventSorter(tablet).sortByTimestampIfNecessary();
    }

    try {
      if (isTabletEmpty(tablet)) {
        // Empty statement, will be filtered after construction
        return new InsertTabletStatement();
      }

      return new InsertTabletStatement(tablet, isAligned, dataBaseName);
    } catch (final MetadataException e) {
      LOGGER.warn("Generate Statement from tablet {} error.", tablet, e);
      return null;
    }
  }

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeTransferTabletRawReqV2 toTPipeTransferRawReq(
      final Tablet tablet, final boolean isAligned, final String dataBaseName) {
    final PipeTransferTabletRawReqV2 tabletReq = new PipeTransferTabletRawReqV2();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;
    tabletReq.dataBaseName = dataBaseName;
    tabletReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    tabletReq.type = PipeRequestType.TRANSFER_TABLET_RAW_V2.getType();

    return tabletReq;
  }

  public static PipeTransferTabletRawReqV2 toTPipeTransferRawReq(final ByteBuffer buffer) {
    final PipeTransferTabletRawReqV2 tabletReq = new PipeTransferTabletRawReqV2();

    tabletReq.deserializeTPipeTransferRawReq(buffer);
    tabletReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    tabletReq.type = PipeRequestType.TRANSFER_TABLET_RAW_V2.getType();

    return tabletReq;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletRawReqV2 toTPipeTransferReq(
      final Tablet tablet, final boolean isAligned, final String dataBaseName) throws IOException {
    final PipeTransferTabletRawReqV2 tabletReq = new PipeTransferTabletRawReqV2();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;
    tabletReq.dataBaseName = dataBaseName;

    tabletReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    tabletReq.type = PipeRequestType.TRANSFER_TABLET_RAW_V2.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      ReadWriteIOUtils.write(dataBaseName, outputStream);
      tabletReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return tabletReq;
  }

  public static PipeTransferTabletRawReqV2 fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferTabletRawReqV2 tabletReq = new PipeTransferTabletRawReqV2();

    tabletReq.deserializeTPipeTransferRawReq(transferReq.body);
    tabletReq.body = transferReq.body;

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;

    return tabletReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final Tablet tablet, final boolean isAligned, final String dataBaseName) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_RAW_V2.getType(), outputStream);
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      ReadWriteIOUtils.write(dataBaseName, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final PipeTransferTabletRawReqV2 that = (PipeTransferTabletRawReqV2) o;
    return Objects.equals(dataBaseName, that.dataBaseName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), dataBaseName);
  }

  /////////////////////////////// Util ///////////////////////////////

  public void deserializeTPipeTransferRawReq(final ByteBuffer buffer) {
    final int startPosition = buffer.position();
    try {
      // V2: read databaseName, readDatabaseName = true
      final InsertTabletStatement insertTabletStatement =
          TabletStatementConverter.deserializeStatementFromTabletFormat(buffer, true);
      this.isAligned = insertTabletStatement.isAligned();
      // databaseName is already set in deserializeStatementFromTabletFormat when
      // readDatabaseName=true
      this.dataBaseName = insertTabletStatement.getDatabaseName().orElse(null);
      this.statement = insertTabletStatement;
    } catch (final Exception e) {
      // If Statement deserialization fails, fallback to Tablet format
      // Reset buffer position for Tablet deserialization
      buffer.position(startPosition);
      this.tablet = Tablet.deserialize(buffer);
      this.isAligned = ReadWriteIOUtils.readBool(buffer);
      this.dataBaseName = ReadWriteIOUtils.readString(buffer);
    }
  }
}
