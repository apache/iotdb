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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.pipe.sink.util.TabletStatementConverter;
import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTreeModelTabletEventSorter;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.enums.TSDataType;
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
        LOGGER.warn(DataNodePipeMessages.FAILED_TO_CONVERT_STATEMENT_TO_TABLET, e);
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
      LOGGER.warn(DataNodePipeMessages.GENERATE_STATEMENT_FROM_TABLET_ERROR, tablet, e);
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

  public static PipeTransferTabletRawReq toTPipeTransferRawReq(
      final ByteBuffer buffer, final TabletStringInternPool tabletStringInternPool) {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.deserializeTPipeTransferRawReq(buffer, tabletStringInternPool);

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
    final PipeTransferTabletRawReq tabletReq =
        toTPipeTransferRawReq(transferReq.body, new TabletStringInternPool());

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;

    return tabletReq;
  }

  private void deserializeTPipeTransferRawReq(
      final ByteBuffer buffer, final TabletStringInternPool tabletStringInternPool) {
    final int startPosition = buffer.position();
    try {
      // Current V1 raw tablet requests can be converted to InsertTabletStatement directly. Keep
      // this as the first attempt to avoid the overhead of constructing an intermediate Tablet.
      final InsertTabletStatement insertTabletStatement =
          TabletStatementConverter.deserializeStatementFromTabletFormat(
              buffer, false, tabletStringInternPool);
      // Legacy tablets do not serialize column categories. Since hasSchema=1 can be
      // misread as FIELD, the current reader may return a corrupt statement instead of failing.
      ensureStatementDeserializedFromCurrentTabletFormat(insertTabletStatement);
      isAligned = insertTabletStatement.isAligned();
      statement = insertTabletStatement;
      return;
    } catch (final Exception e) {
      buffer.position(startPosition);
    }

    try {
      // Some old senders serialize Tablet without column categories. Retry with the legacy reader
      // before falling back to the full Tablet deserialization path.
      final InsertTabletStatement insertTabletStatement =
          TabletStatementConverter.deserializeLegacyStatementFromTabletFormat(
              buffer, tabletStringInternPool);
      isAligned = insertTabletStatement.isAligned();
      statement = insertTabletStatement;
      return;
    } catch (final Exception e) {
      buffer.position(startPosition);
    }

    try {
      tablet = PipeTabletUtils.internTablet(Tablet.deserialize(buffer), tabletStringInternPool);
      isAligned = ReadWriteIOUtils.readBool(buffer);
    } catch (final RuntimeException e) {
      buffer.position(startPosition);
      throw new IllegalArgumentException(
          String.format(
              DataNodePipeMessages
                  .EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_REQUEST_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_45AC3692,
              startPosition,
              buffer.remaining()),
          e);
    }
  }

  private static void ensureStatementDeserializedFromCurrentTabletFormat(
      final InsertTabletStatement statement) {
    final String[] measurements = statement.getMeasurements();
    final TSDataType[] dataTypes = statement.getDataTypes();

    if (Objects.isNull(measurements)
        || Objects.isNull(dataTypes)
        || measurements.length != dataTypes.length) {
      throw new IllegalArgumentException(
          DataNodePipeMessages
              .EXCEPTION_INCOMPLETE_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_A23A1C30);
    }

    final Object[] columns = statement.getColumns();
    if (Objects.nonNull(columns) && columns.length != measurements.length) {
      throw new IllegalArgumentException(
          DataNodePipeMessages
              .EXCEPTION_COLUMN_COUNT_IS_INCONSISTENT_WITH_SCHEMA_COUNT_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_53BA037A);
    }

    for (int i = 0; i < measurements.length; ++i) {
      if (Objects.isNull(measurements[i]) || Objects.isNull(dataTypes[i])) {
        throw new IllegalArgumentException(
            DataNodePipeMessages
                .EXCEPTION_INCOMPLETE_MEASUREMENT_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_B8DB28A8);
      }
      if (statement.getRowCount() > 0 && (Objects.isNull(columns) || Objects.isNull(columns[i]))) {
        throw new IllegalArgumentException(
            DataNodePipeMessages
                .EXCEPTION_INCOMPLETE_COLUMN_VALUES_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_269782B9);
      }
    }

    final long[] times = statement.getTimes();
    if (statement.getRowCount() > 0
        && measurements.length > 0
        && (Objects.isNull(times) || times.length < statement.getRowCount())) {
      throw new IllegalArgumentException(
          DataNodePipeMessages
              .EXCEPTION_INCOMPLETE_TIMESTAMPS_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_FE212461);
    }
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
        throw new IOException(DataNodePipeMessages.FAILED_TO_CONVERT_STATEMENT_TO_TABLET_FOR, e);
      }
    }

    if (tabletToSerialize == null) {
      throw new IOException(DataNodePipeMessages.CANNOT_SERIALIZE_BOTH_TABLET_AND_STATEMENT_ARE);
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
