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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.request;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.pipe.connector.util.PipeTabletEventSorter;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent.isTabletEmpty;

public class PipeTransferTabletRawReqV2 extends PipeTransferTabletRawReq {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTabletRawReqV2.class);

  protected transient String dataBaseName;

  public String getDataBaseName() {
    return dataBaseName;
  }

  @Override
  public InsertTabletStatement constructStatement() {
    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    try {
      if (isTabletEmpty(tablet)) {
        // Empty statement, will be filtered after construction
        return new InsertTabletStatement();
      }

      final TSInsertTabletReq request = new TSInsertTabletReq();

      for (final IMeasurementSchema measurementSchema : tablet.getSchemas()) {
        request.addToMeasurements(measurementSchema.getMeasurementName());
        request.addToTypes(measurementSchema.getType().ordinal());
      }

      request.setPrefixPath(tablet.getDeviceId());
      request.setIsAligned(isAligned);
      request.setTimestamps(SessionUtils.getTimeBuffer(tablet));
      request.setValues(SessionUtils.getValueBuffer(tablet));
      request.setSize(tablet.getRowSize());

      // Tree model
      if (Objects.isNull(dataBaseName)) {
        request.setMeasurements(
            PathUtils.checkIsLegalSingleMeasurementsAndUpdate(request.getMeasurements()));
        return StatementGenerator.createStatement(request);
      }

      // Table model
      request.setWriteToTable(true);
      request.columnCategories =
          tablet.getColumnTypes().stream()
              .map(t -> (byte) t.ordinal())
              .collect(Collectors.toList());
      final InsertTabletStatement statement = StatementGenerator.createStatement(request);
      statement.setDatabaseName(dataBaseName);
      return statement;
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
    tabletReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
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

    tabletReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
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

    tabletReq.tablet = Tablet.deserialize(transferReq.body);
    tabletReq.isAligned = ReadWriteIOUtils.readBool(transferReq.body);
    tabletReq.dataBaseName = ReadWriteIOUtils.readString(transferReq.body);

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;
    tabletReq.body = transferReq.body;

    return tabletReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final Tablet tablet, final boolean isAligned, final String dataBaseName) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
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
}
