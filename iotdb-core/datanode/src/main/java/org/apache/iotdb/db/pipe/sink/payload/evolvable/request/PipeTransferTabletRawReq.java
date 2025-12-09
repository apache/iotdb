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

  protected transient InsertTabletStatement statement;

  protected transient boolean isAligned;
  protected transient Tablet tablet;

  /**
   * Get Tablet. If tablet is null, convert from statement.
   *
   * @return Tablet object
   */
  public Tablet getTablet() {
    if (tablet == null && statement != null) {
      try {
        tablet = statement.convertToTablet();
      } catch (final MetadataException e) {
        LOGGER.warn("Failed to convert statement to tablet.", e);
        return null;
      }
    }
    return tablet;
  }

  public boolean getIsAligned() {
    return isAligned;
  }

  /**
   * Construct Statement. If statement already exists, return it. Otherwise, convert from tablet.
   *
   * @return InsertTabletStatement
   */
  public InsertTabletStatement constructStatement() {
    if (statement != null) {
      new PipeTreeModelTabletEventSorter(statement).deduplicateAndSortTimestampsIfNecessary();
      return statement;
    }

    // Sort and deduplicate tablet before converting
    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    try {
      if (isTabletEmpty(tablet)) {
        // Empty statement, will be filtered after construction
        statement = new InsertTabletStatement();
        return statement;
      }

      statement = new InsertTabletStatement(tablet, isAligned, null);
      return statement;
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

    final ByteBuffer buffer = transferReq.body;
    final int startPosition = buffer.position();
    try {
      // V1: no databaseName, readDatabaseName = false
      final InsertTabletStatement insertTabletStatement =
          TabletStatementConverter.deserializeStatementFromTabletFormat(buffer, false);
      tabletReq.isAligned = insertTabletStatement.isAligned();
      // devicePath is already set in deserializeStatementFromTabletFormat for V1 format
      tabletReq.statement = insertTabletStatement;
    } catch (final Exception e) {
      buffer.position(startPosition);
      tabletReq.tablet = Tablet.deserialize(buffer);
      tabletReq.isAligned = ReadWriteIOUtils.readBool(buffer);
    }

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;

    return tabletReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  /**
   * Serialize to bytes. If tablet is null, convert from statement first.
   *
   * @return serialized bytes
   * @throws IOException if serialization fails
   */
  public byte[] toTPipeTransferBytes() throws IOException {
    Tablet tabletToSerialize = tablet;
    boolean isAlignedToSerialize = isAligned;

    // If tablet is null, convert from statement
    if (tabletToSerialize == null && statement != null) {
      try {
        tabletToSerialize = statement.convertToTablet();
        isAlignedToSerialize = statement.isAligned();
      } catch (final MetadataException e) {
        throw new IOException("Failed to convert statement to tablet for serialization", e);
      }
    }

    if (tabletToSerialize == null) {
      throw new IOException("Cannot serialize: both tablet and statement are null");
    }

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_RAW.getType(), outputStream);
      tabletToSerialize.serialize(outputStream);
      ReadWriteIOUtils.write(isAlignedToSerialize, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  /**
   * Static method for backward compatibility. Creates a temporary instance and serializes.
   *
   * @param tablet Tablet to serialize
   * @param isAligned whether aligned
   * @return serialized bytes
   * @throws IOException if serialization fails
   */
  public static byte[] toTPipeTransferBytes(final Tablet tablet, final boolean isAligned)
      throws IOException {
    final PipeTransferTabletRawReq req = new PipeTransferTabletRawReq();
    req.tablet = tablet;
    req.isAligned = isAligned;
    return req.toTPipeTransferBytes();
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
    // Compare statement if both have it, otherwise compare tablet
    if (statement != null && that.statement != null) {
      return Objects.equals(statement, that.statement)
          && isAligned == that.isAligned
          && version == that.version
          && type == that.type
          && Objects.equals(body, that.body);
    }
    // Fallback to tablet comparison
    return Objects.equals(getTablet(), that.getTablet())
        && isAligned == that.isAligned
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTablet(), isAligned, version, type, body);
  }
}
