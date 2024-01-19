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
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class PipeTransferTabletRawReq extends TPipeTransferReq {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTabletRawReq.class);

  private transient Tablet tablet;
  private transient boolean isAligned;

  public Tablet getTablet() {
    return tablet;
  }

  public boolean getIsAligned() {
    return isAligned;
  }

  public InsertTabletStatement constructStatement() {
    if (!checkSorted(tablet)) {
      sortTablet(tablet);
    }

    try {
      final TSInsertTabletReq request = new TSInsertTabletReq();

      for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
        request.addToMeasurements(measurementSchema.getMeasurementId());
        request.addToTypes(measurementSchema.getType().ordinal());
      }

      request.setPrefixPath(tablet.deviceId);
      request.setIsAligned(isAligned);
      request.setTimestamps(SessionUtils.getTimeBuffer(tablet));
      request.setValues(SessionUtils.getValueBuffer(tablet));
      request.setSize(tablet.rowSize);
      request.setMeasurements(
          PathUtils.checkIsLegalSingleMeasurementsAndUpdate(request.getMeasurements()));

      return StatementGenerator.createStatement(request);
    } catch (MetadataException e) {
      LOGGER.warn("Generate Statement from tablet {} error.", tablet, e);
      return null;
    }
  }

  private static boolean checkSorted(Tablet tablet) {
    for (int i = 1; i < tablet.rowSize; i++) {
      if (tablet.timestamps[i] < tablet.timestamps[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private static void sortTablet(Tablet tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    Integer[] index = new Integer[tablet.rowSize];
    for (int i = 0; i < tablet.rowSize; i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparingLong(o -> tablet.timestamps[o]));
    Arrays.sort(tablet.timestamps, 0, tablet.rowSize);
    int columnIndex = 0;
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema != null) {
        tablet.values[columnIndex] = sortList(tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      }
    }
  }

  /**
   * Sort value list by index.
   *
   * @param valueList value list
   * @param dataType data type
   * @param index index
   * @return sorted list
   * @throws UnSupportedDataTypeException if dataType is illegal
   */
  private static Object sortList(Object valueList, TSDataType dataType, Integer[] index) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        boolean[] sortedValues = new boolean[boolValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedValues[i] = boolValues[index[i]];
        }
        return sortedValues;
      case INT32:
        int[] intValues = (int[]) valueList;
        int[] sortedIntValues = new int[intValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedIntValues[i] = intValues[index[i]];
        }
        return sortedIntValues;
      case INT64:
        long[] longValues = (long[]) valueList;
        long[] sortedLongValues = new long[longValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedLongValues[i] = longValues[index[i]];
        }
        return sortedLongValues;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        float[] sortedFloatValues = new float[floatValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedFloatValues[i] = floatValues[index[i]];
        }
        return sortedFloatValues;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        double[] sortedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedDoubleValues[i] = doubleValues[index[i]];
        }
        return sortedDoubleValues;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        Binary[] sortedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedBinaryValues[i] = binaryValues[index[i]];
        }
        return sortedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  /**
   * Sort BitMap by index.
   *
   * @param bitMap BitMap to be sorted
   * @param index index
   * @return sorted bitMap
   */
  private static BitMap sortBitMap(BitMap bitMap, Integer[] index) {
    BitMap sortedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < index.length; i++) {
      if (bitMap.isMarked(index[i])) {
        sortedBitMap.mark(i);
      }
    }
    return sortedBitMap;
  }

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeTransferTabletRawReq toTPipeTransferRawReq(Tablet tablet, boolean isAligned) {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;

    return tabletReq;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletRawReq toTPipeTransferReq(Tablet tablet, boolean isAligned)
      throws IOException {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.tablet = tablet;
    tabletReq.isAligned = isAligned;

    tabletReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
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

  public static PipeTransferTabletRawReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferTabletRawReq tabletReq = new PipeTransferTabletRawReq();

    tabletReq.tablet = Tablet.deserialize(transferReq.body);
    tabletReq.isAligned = ReadWriteIOUtils.readBool(transferReq.body);

    tabletReq.version = transferReq.version;
    tabletReq.type = transferReq.type;
    tabletReq.body = transferReq.body;

    return tabletReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(Tablet tablet, boolean isAligned) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_RAW.getType(), outputStream);
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(isAligned, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
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
    PipeTransferTabletRawReq that = (PipeTransferTabletRawReq) obj;
    return tablet.equals(that.tablet)
        && isAligned == that.isAligned
        && version == that.version
        && type == that.type
        && (body == null || that.body == null ? body == that.body : body.equals(that.body));
  }

  @Override
  public int hashCode() {
    return Objects.hash(tablet, isAligned, version, type, body);
  }
}
