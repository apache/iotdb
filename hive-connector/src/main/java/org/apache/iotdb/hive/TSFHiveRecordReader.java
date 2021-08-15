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
package org.apache.iotdb.hive;

import org.apache.iotdb.hadoop.tsfile.IReaderSet;
import org.apache.iotdb.hadoop.tsfile.TSFInputSplit;
import org.apache.iotdb.hadoop.tsfile.TSFRecordReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.hadoop.tsfile.TSFRecordReader.getCurrentValue;

public class TSFHiveRecordReader implements RecordReader<NullWritable, MapWritable>, IReaderSet {

  private static final Logger logger = LoggerFactory.getLogger(TSFHiveRecordReader.class);

  /** all datasets corresponding to one specific split */
  private List<QueryDataSet> dataSetList = new ArrayList<>();
  /**
   * List for name of devices. The order corresponds to the order of dataSetList. Means that
   * deviceIdList[i] is the name of device for dataSetList[i].
   */
  private List<String> deviceIdList = new ArrayList<>();
  /** The index of QueryDataSet that is currently processed */
  private int currentIndex = 0;

  private boolean isReadDeviceId;
  private boolean isReadTime;
  private TsFileSequenceReader reader;
  private List<String> measurementIds;

  @Override
  public boolean next(NullWritable key, MapWritable value) throws IOException {
    while (currentIndex < dataSetList.size()) {
      if (!dataSetList.get(currentIndex).hasNext()) {
        currentIndex++;
      } else {
        RowRecord rowRecord = dataSetList.get(currentIndex).next();
        List<Field> fields = rowRecord.getFields();
        long timestamp = rowRecord.getTimestamp();

        try {
          MapWritable res = new MapWritable();
          getCurrentValue(
                  deviceIdList,
                  currentIndex,
                  timestamp,
                  isReadTime,
                  isReadDeviceId,
                  fields,
                  measurementIds)
              .forEach((k, v) -> res.put(new Text(k.toString().toLowerCase()), v));
          value.putAll(res);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e.getMessage());
        }

        return true;
      }
    }
    return false;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() {
    // can't know
    return 0;
  }

  public TSFHiveRecordReader(InputSplit split, JobConf job) throws IOException {
    if (split instanceof TSFInputSplit) {
      TSFRecordReader.initialize((TSFInputSplit) split, job, this, dataSetList, deviceIdList);
    } else {
      logger.error(
          "The InputSplit class is not {}, the class is {}",
          TSFInputSplit.class.getName(),
          split.getClass().getName());
      throw new InternalError(
          String.format(
              "The InputSplit class is not %s, the class is %s",
              TSFInputSplit.class.getName(), split.getClass().getName()));
    }
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    dataSetList = null;
    deviceIdList = null;
    reader.close();
  }

  @Override
  public void setReader(TsFileSequenceReader reader) {
    this.reader = reader;
  }

  @Override
  public void setMeasurementIds(List<String> measurementIds) {
    this.measurementIds = measurementIds;
  }

  @Override
  public void setReadDeviceId(boolean isReadDeviceId) {
    this.isReadDeviceId = isReadDeviceId;
  }

  @Override
  public void setReadTime(boolean isReadTime) {
    this.isReadTime = isReadTime;
  }
}
