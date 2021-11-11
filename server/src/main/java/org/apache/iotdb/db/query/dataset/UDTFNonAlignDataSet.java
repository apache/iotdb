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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDTFNonAlignDataSet extends UDTFDataSet implements DirectNonAlignDataSet {

  protected boolean isInitialized;

  protected int[] alreadyReturnedRowNumArray;
  protected int[] offsetArray;

  /** execute with value filter */
  public UDTFNonAlignDataSet(
      QueryContext context,
      UDTFPlan udtfPlan,
      TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries,
      List<Boolean> cached)
      throws IOException, QueryProcessException {
    super(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        readersOfSelectedSeries,
        cached);
    isInitialized = false;
  }

  /** execute without value filter */
  public UDTFNonAlignDataSet(
      QueryContext context, UDTFPlan udtfPlan, List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException, IOException, InterruptedException {
    super(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        readersOfSelectedSeries);
    isInitialized = false;
  }

  /**
   * offsetArray can not be initialized in the constructor, because rowOffset is set after the
   * DataSet's construction. note that this method only can be called once in a query.
   */
  protected void init() {
    alreadyReturnedRowNumArray = new int[transformers.length]; // all elements are 0
    offsetArray = new int[transformers.length];
    Arrays.fill(offsetArray, rowOffset);
    isInitialized = true;
  }

  @Override
  public TSQueryNonAlignDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder)
      throws IOException, QueryProcessException {
    if (!isInitialized) {
      init();
    }

    TSQueryNonAlignDataSet tsQueryNonAlignDataSet = new TSQueryNonAlignDataSet();

    int columnsNum = transformers.length;
    List<ByteBuffer> timeBufferList = new ArrayList<>(columnsNum);
    List<ByteBuffer> valueBufferList = new ArrayList<>(columnsNum);

    for (int i = 0; i < columnsNum; ++i) {
      Pair<ByteBuffer, ByteBuffer> timeValueByteBufferPair =
          fillColumnBuffer(i, fetchSize, encoder); // todo: parallelization
      timeBufferList.add(timeValueByteBufferPair.left);
      valueBufferList.add(timeValueByteBufferPair.right);
    }

    rawQueryInputLayer.updateRowRecordListEvictionUpperBound();

    tsQueryNonAlignDataSet.setTimeList(timeBufferList);
    tsQueryNonAlignDataSet.setValueList(valueBufferList);
    return tsQueryNonAlignDataSet;
  }

  protected Pair<ByteBuffer, ByteBuffer> fillColumnBuffer(
      int transformedDataColumnIndex, int fetchSize, WatermarkEncoder encoder)
      throws IOException, QueryProcessException {
    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS valueBAOS = new PublicBAOS();

    LayerPointReader reader = transformers[transformedDataColumnIndex];
    TSDataType type = reader.getDataType();
    int rowCount = 0;
    while (rowCount < fetchSize
        && (rowLimit <= 0 || alreadyReturnedRowNumArray[transformedDataColumnIndex] < rowLimit)
        && reader.next()) {

      if (offsetArray[transformedDataColumnIndex] == 0) {

        long timestamp = reader.currentTime();
        ReadWriteIOUtils.write(timestamp, timeBAOS);

        switch (type) {
          case INT32:
            int intValue = reader.currentInt();
            ReadWriteIOUtils.write(
                encoder != null && encoder.needEncode(timestamp)
                    ? encoder.encodeInt(intValue, timestamp)
                    : intValue,
                valueBAOS);
            break;
          case INT64:
            long longValue = reader.currentLong();
            ReadWriteIOUtils.write(
                encoder != null && encoder.needEncode(timestamp)
                    ? encoder.encodeLong(longValue, timestamp)
                    : longValue,
                valueBAOS);
            break;
          case FLOAT:
            float floatValue = reader.currentFloat();
            ReadWriteIOUtils.write(
                encoder != null && encoder.needEncode(timestamp)
                    ? encoder.encodeFloat(floatValue, timestamp)
                    : floatValue,
                valueBAOS);
            break;
          case DOUBLE:
            double doubleValue = reader.currentDouble();
            ReadWriteIOUtils.write(
                encoder != null && encoder.needEncode(timestamp)
                    ? encoder.encodeDouble(doubleValue, timestamp)
                    : doubleValue,
                valueBAOS);
            break;
          case BOOLEAN:
            ReadWriteIOUtils.write(reader.currentBoolean(), valueBAOS);
            break;
          case TEXT:
            ReadWriteIOUtils.write(reader.currentBinary(), valueBAOS);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      }

      reader.readyForNext();

      if (offsetArray[transformedDataColumnIndex] == 0) {
        ++rowCount;
        if (rowLimit > 0) {
          ++alreadyReturnedRowNumArray[transformedDataColumnIndex];
        }
      } else {
        --offsetArray[transformedDataColumnIndex];
      }
    }

    ByteBuffer timeBuffer = ByteBuffer.wrap(timeBAOS.getBuf());
    timeBuffer.limit(timeBAOS.size());
    ByteBuffer valueBuffer = ByteBuffer.wrap(valueBAOS.getBuf());
    valueBuffer.limit(valueBAOS.size());
    return new Pair<>(timeBuffer, valueBuffer);
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    throw new NotImplementedException("UDTFNonAlignDataSet#hasNextWithoutConstraint");
  }

  @Override
  public RowRecord nextWithoutConstraint() {
    throw new NotImplementedException("UDTFNonAlignDataSet#nextWithoutConstraint");
  }
}
