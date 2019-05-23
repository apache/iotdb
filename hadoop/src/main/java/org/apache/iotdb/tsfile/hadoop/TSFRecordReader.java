/**
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
package org.apache.iotdb.tsfile.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iotdb.tsfile.file.metadata.RowGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TimeSeriesChunkMetaData;
import org.apache.iotdb.tsfile.hadoop.io.HDFSInputStream;
import org.apache.iotdb.tsfile.read.query.HadoopQueryEngine;
import org.apache.iotdb.tsfile.read.query.QueryDataSet;
import org.apache.iotdb.tsfile.read.support.Field;
import org.apache.iotdb.tsfile.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liukun
 */
public class TSFRecordReader extends RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSFRecordReader.class);

  private QueryDataSet dataSet = null;
  private List<Field> fields = null;
  private long timestamp = 0;
  private String deviceId;
  private int sensorNum = 0;
  private int sensorIndex = 0;
  private boolean isReadDeviceId = false;
  private boolean isReadTime = false;
  private int arraySize = 0;
  private HDFSInputStream hdfsInputStream;


  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (split instanceof TSFInputSplit) {
      TSFInputSplit tsfInputSplit = (TSFInputSplit) split;
      Path path = tsfInputSplit.getPath();
      List<RowGroupMetaData> rowGroupMetaDataList = tsfInputSplit.getDeviceRowGroupMetaDataList();
      Configuration configuration = context.getConfiguration();
      hdfsInputStream = new HDFSInputStream(path, configuration);
      // Get the read columns and filter information
      List<String> deltaObjectIdsList = TSFInputFormat.getReadDeltaObjectIds(configuration);
      if (deltaObjectIdsList == null) {
        deltaObjectIdsList = initDeviceIdList(rowGroupMetaDataList);
      }
      List<String> measurementIdsList = TSFInputFormat.getReadMeasurementIds(configuration);
      if (measurementIdsList == null) {
        measurementIdsList = initSensorIdList(rowGroupMetaDataList);
      }
      LOGGER.info("deltaObjectIds:" + deltaObjectIdsList);
      LOGGER.info("Sensors:" + measurementIdsList);

      this.sensorNum = measurementIdsList.size();
      isReadDeviceId = TSFInputFormat.getReadDeltaObject(configuration);
      isReadTime = TSFInputFormat.getReadTime(configuration);
      if (isReadDeviceId) {
        arraySize++;
      }
      if (isReadTime) {
        arraySize++;
      }
      arraySize += sensorNum;

      HadoopQueryEngine queryEngine = new HadoopQueryEngine(hdfsInputStream, rowGroupMetaDataList);
      dataSet = queryEngine
          .queryWithSpecificRowGroups(deltaObjectIdsList, measurementIdsList, null, null, null);
    } else {
      LOGGER.error("The InputSplit class is not {}, the class is {}", TSFInputSplit.class.getName(),
          split.getClass().getName());
      throw new InternalError(String.format("The InputSplit class is not %s, the class is %s",
          TSFInputSplit.class.getName(), split.getClass().getName()));
    }
  }

  private List<String> initDeviceIdList(List<RowGroupMetaData> rowGroupMetaDataList) {
    Set<String> deviceIdSet = new HashSet<>();
    for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
      deviceIdSet.add(rowGroupMetaData.getDeltaObjectID());
    }
    return new ArrayList<>(deviceIdSet);
  }

  private List<String> initSensorIdList(List<RowGroupMetaData> rowGroupMetaDataList) {
    Set<String> sensorIdSet = new HashSet<>();
    for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
      for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData
          .getTimeSeriesChunkMetaDataList()) {
        sensorIdSet.add(timeSeriesChunkMetaData.getProperties().getMeasurementUID());
      }
    }
    return new ArrayList<>(sensorIdSet);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    sensorIndex += sensorNum;

    if (fields == null || sensorIndex >= fields.size()) {
      LOGGER.info("Start another row~");
      if (!dataSet.next()) {
        LOGGER.info("Finish all rows~");
        return false;
      }

      RowRecord rowRecord = dataSet.getCurrentRecord();
      fields = rowRecord.getFields();
      timestamp = rowRecord.getTime();
      sensorIndex = 0;
    }
    deviceId = fields.get(sensorIndex).deltaObjectId;

    return true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public ArrayWritable getCurrentValue() throws IOException, InterruptedException {

    Writable[] writables = getEmptyWritables();
    Text deviceid = new Text(deviceId);
    LongWritable time = new LongWritable(timestamp);
    int index = 0;
    if (isReadTime && isReadDeviceId) {
      writables[0] = time;
      writables[1] = deviceid;
      index = 2;
    } else if (isReadTime && !isReadDeviceId) {
      writables[0] = time;
      index = 1;
    } else if (!isReadTime && isReadDeviceId) {
      writables[0] = deviceid;
      index = 1;
    }

    for (int i = 0; i < sensorNum; i++) {
      Field field = fields.get(sensorIndex + i);
      if (field.isNull()) {
        LOGGER.info("Current value is null");
        writables[index] = NullWritable.get();
      } else {
        switch (field.dataType) {
          case INT32:
            writables[index] = new IntWritable(field.getIntV());
            break;
          case INT64:
            writables[index] = new LongWritable(field.getLongV());
            break;
          case FLOAT:
            writables[index] = new FloatWritable(field.getFloatV());
            break;
          case DOUBLE:
            writables[index] = new DoubleWritable(field.getDoubleV());
            break;
          case BOOLEAN:
            writables[index] = new BooleanWritable(field.getBoolV());
            break;
          case TEXT:
            writables[index] = new Text(field.getBinaryV().getStringValue());
            break;
          default:
            LOGGER.error("The data type is not support {}", field.dataType);
            throw new InterruptedException(
                String.format("The data type %s is not support ", field.dataType));
        }
      }
      index++;
    }
    return new ArrayWritable(Writable.class, writables);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    dataSet = null;
    hdfsInputStream.close();
  }

  private Writable[] getEmptyWritables() {
    Writable[] writables = new Writable[arraySize];
    return writables;
  }
}
