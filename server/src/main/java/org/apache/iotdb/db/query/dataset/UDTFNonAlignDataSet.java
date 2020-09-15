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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class UDTFNonAlignDataSet extends UDTFDataSet implements DirectNonAlignDataSet {

  protected int[] alreadyReturnedRowNumArray;
  protected int[] offsetArray;

  // execute with value filters
  public UDTFNonAlignDataSet(QueryContext context, UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries, List<Boolean> cached)
      throws IOException, QueryProcessException {
    super(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes, timestampGenerator,
        readersOfSelectedSeries, cached);
    init();
  }

  // execute without value filters
  public UDTFNonAlignDataSet(QueryContext context, UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException {
    super(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes, readersOfSelectedSeries);
    init();
  }

  protected void init() {
    int columnsNum = transformedDataColumns.length;
    alreadyReturnedRowNumArray = new int[columnsNum];
    offsetArray = new int[columnsNum];
    Arrays.fill(offsetArray, rowOffset);
  }

  @Override
  public TSQueryNonAlignDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder)
      throws IOException, InterruptedException {
    TSQueryNonAlignDataSet tsQueryNonAlignDataSet = new TSQueryNonAlignDataSet();

    int columnsNum = transformedDataColumns.length;
    CountDownLatch countDownLatch = new CountDownLatch(columnsNum);
    List<ByteBuffer> timeBufferList = new ArrayList<>(columnsNum);
    List<ByteBuffer> valueBufferList = new ArrayList<>(columnsNum);
    for (int i = 0; i < columnsNum; ++i) {
      timeBufferList.add(null);
      valueBufferList.add(null);
    }
    for (int i = 0; i < columnsNum; ++i) {
      TASK_POOL_MANAGER.submit(
          new FillBufferTask(i, fetchSize, encoder, timeBufferList, valueBufferList,
              countDownLatch));
    }
    countDownLatch.await();

    tsQueryNonAlignDataSet.setTimeList(timeBufferList);
    tsQueryNonAlignDataSet.setValueList(valueBufferList);
    return tsQueryNonAlignDataSet;
  }

  protected Pair<ByteBuffer, ByteBuffer> fillColumnBuffer(int transformedDataColumnIndex,
      int fetchSize, WatermarkEncoder encoder) throws IOException {
    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS valueBAOS = new PublicBAOS();

    DataPointIterator dataPointIterator = transformedDataColumns[transformedDataColumnIndex];
    TSDataType type = transformedDataColumnDataTypes[transformedDataColumnIndex];
    int rowCount = 0;
    while (rowCount < fetchSize
        && (rowLimit <= 0 || alreadyReturnedRowNumArray[transformedDataColumnIndex] < rowLimit)
        && dataPointIterator.hasNextPoint()) {

      if (offsetArray[transformedDataColumnIndex] == 0) {
        dataPointIterator.next();

        long timestamp = dataPointIterator.currentTime();
        ReadWriteIOUtils.write(timestamp, timeBAOS);

        switch (type) {
          case INT32:
            int intValue = dataPointIterator.currentInt();
            ReadWriteIOUtils.write(encoder != null && encoder.needEncode(timestamp)
                ? encoder.encodeInt(intValue, timestamp) : intValue, valueBAOS);
            break;
          case INT64:
            long longValue = dataPointIterator.currentLong();
            ReadWriteIOUtils.write(encoder != null && encoder.needEncode(timestamp)
                ? encoder.encodeLong(longValue, timestamp) : longValue, valueBAOS);
            break;
          case FLOAT:
            float floatValue = dataPointIterator.currentFloat();
            ReadWriteIOUtils.write(encoder != null && encoder.needEncode(timestamp)
                ? encoder.encodeFloat(floatValue, timestamp) : floatValue, valueBAOS);
            break;
          case DOUBLE:
            double doubleValue = dataPointIterator.currentDouble();
            ReadWriteIOUtils.write(encoder != null && encoder.needEncode(timestamp)
                ? encoder.encodeDouble(doubleValue, timestamp) : doubleValue, valueBAOS);
            break;
          case BOOLEAN:
            ReadWriteIOUtils.write(dataPointIterator.currentBoolean(), valueBAOS);
            break;
          case TEXT:
            ReadWriteIOUtils.write(dataPointIterator.currentBinary(), valueBAOS);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      }

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
  protected boolean hasNextWithoutConstraint() {
    throw new NotImplementedException();
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    throw new NotImplementedException();
  }

  private class FillBufferTask extends WrappedRunnable {

    private final int transformedDataColumnIndex;
    private final int fetchSize;
    private final WatermarkEncoder encoder;
    private final List<ByteBuffer> timeBufferList;
    private final List<ByteBuffer> valueBufferList;

    private final CountDownLatch countDownLatch;

    public FillBufferTask(int transformedDataColumnIndex, int fetchSize, WatermarkEncoder encoder,
        List<ByteBuffer> timeBufferList, List<ByteBuffer> valueBufferList,
        CountDownLatch countDownLatch) {
      this.transformedDataColumnIndex = transformedDataColumnIndex;
      this.fetchSize = fetchSize;
      this.encoder = encoder;
      this.timeBufferList = timeBufferList;
      this.valueBufferList = valueBufferList;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void runMayThrow() throws Exception {
      try {
        Pair<ByteBuffer, ByteBuffer> timeValueByteBufferPair = fillColumnBuffer(
            transformedDataColumnIndex, fetchSize, encoder);
        timeBufferList.set(transformedDataColumnIndex, timeValueByteBufferPair.left);
        valueBufferList.set(transformedDataColumnIndex, timeValueByteBufferPair.right);
      } finally {
        countDownLatch.countDown();
      }
    }
  }
}
