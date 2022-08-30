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

package org.apache.iotdb.datax.writer.iotdbwriter;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IoTDBWriter extends Writer {
  public static class Task extends com.alibaba.datax.common.spi.Writer.Task {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBWriter.Task.class);

    private Configuration conf;

    private List<TSDataType> typeList;
    private List<IoTDBColumn> columnList;
    Session session = null;
    private int batchSize;
    private String username;
    private String password;
    private String host;
    private String storageGroup;
    private String deviceId;
    private int port;
    private int TimeSeriesColumnIndex;

    public Task() {}

    @Override
    public void init() {
      this.conf = super.getPluginJobConf();
      columnList =
          JSON.parseObject(
              this.conf.getString(Key.COLUMN), new TypeReference<List<IoTDBColumn>>() {});
      typeList = new ArrayList<TSDataType>();

      for (IoTDBColumn col : columnList) {
        if (!col.getName().toLowerCase().equals(Key.TIME_SERIES)) {
          typeList.add(TSDataType.valueOf(col.getType().toUpperCase()));
        } else {
          typeList.add(null);
        }
      }
      batchSize = this.conf.getInt(Key.BATCH_SIZE, 10000);
      username = this.conf.getString(Key.USERNAME);
      password = this.conf.getString(Key.PASSWORD);
      storageGroup = this.conf.getString(Key.STORAGE_GROUP);
      deviceId = this.conf.getString(Key.DEVICE_ID);
      host = this.conf.getString(Key.HOST);
      port = this.conf.getInt(Key.PORT);
      //  judge is have timeseries column
      boolean isHaveTimeSeriesColumn = false;
      for (int i = 0; i < columnList.size(); i++) {
        if (columnList.get(i).getName().equals(Key.TIME_SERIES)) {
          TimeSeriesColumnIndex = i;
          isHaveTimeSeriesColumn = true;
        }
      }
      if (!isHaveTimeSeriesColumn) {
        throw new IllegalArgumentException("在writer的column配置中，没有发现命名为 timeseries 的列。 ");
      }
    }

    @Override
    public void prepare() {
      // init session
      session =
          new Session.Builder()
              .host(this.host)
              .port(this.port)
              .username(this.username)
              .password(this.password)
              .version(Version.V_0_13)
              .build();
      try {
        session.open(false);
        // session.setStorageGroup(this.conf.getString(Key.STORAGE_GROUP));
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver) {
      // The schema of measurements of one device
      // only measurementId and data type in MeasurementSchema take effects in Tablet
      List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < columnList.size(); i++) {
        //  if column name timeseries pass
        if (i != TimeSeriesColumnIndex) {
          final IoTDBColumn column = columnList.get(i);
          schemaList.add(
              new MeasurementSchema(
                  column.getName(), TSDataType.valueOf(column.getType().toUpperCase())));
        }
      }

      Tablet tablet = new Tablet(this.deviceId, schemaList, this.batchSize);

      tablet.initBitMaps();

      Record record = null;
      while ((record = recordReceiver.getFromReader()) != null) {
        int rowIndex = tablet.rowSize++;
        //  get timeseries column
        final Column timeSeriesColumn = record.getColumn(TimeSeriesColumnIndex);
        tablet.addTimestamp(rowIndex, timeSeriesColumn.asDate().getTime());

        for (int s = 0; s < record.getColumnNumber(); s++) {
          Column column = record.getColumn(s);
          String columnName = columnList.get(s).getName();
          TSDataType columnType = typeList.get(s);
          //  if column name timeseries pass switch
          if (s != TimeSeriesColumnIndex) {
            //   transform column value according to writer column define
            switch (columnType) {
              case BOOLEAN:
                tablet.addValue(columnName, rowIndex, column.asBoolean());
                break;
              case INT32:
                tablet.addValue(columnName, rowIndex, column.asLong().intValue());
                break;
              case INT64:
                tablet.addValue(columnName, rowIndex, column.asLong());
                break;
              case FLOAT:
                tablet.addValue(columnName, rowIndex, column.asDouble().floatValue());
                break;
              case DOUBLE:
                tablet.addValue(columnName, rowIndex, column.asDouble());
                break;
              case TEXT:
                tablet.addValue(columnName, rowIndex, new Binary(column.asString()));
                break;
              default:
                getTaskPluginCollector()
                    .collectDirtyRecord(record, "类型错误:不支持的类型:" + columnType + " " + columnName);
            }
          }
          // todo   mark null value
          // if (row % 3 == s) {
          //    tablet.bitMaps[s].mark((int) row);
          // }
        }
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          try {
            session.insertTablet(tablet, true);
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
          }
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        try {
          session.insertTablet(tablet);
        } catch (StatementExecutionException | IoTDBConnectionException e) {
          e.printStackTrace();
        }
        tablet.reset();
      }
    }

    @Override
    public void post() {}

    @Override
    public void destroy() {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
    }

    @Override
    public boolean supportFailOver() {
      return false;
    }
  }

  public static class Job extends com.alibaba.datax.common.spi.Writer.Job {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBWriter.Job.class);
    private Configuration originalConfig = null;

    public Job() {}

    @Override
    public void init() {

      this.originalConfig = super.getPluginJobConf();
    }

    @Override
    public void preCheck() {
      String username = this.originalConfig.getString(Key.USERNAME);
      String password = this.originalConfig.getString(Key.PASSWORD);
      String host = this.originalConfig.getString(Key.HOST);
      String port = this.originalConfig.getString(Key.PORT);
      if (StringUtils.isAnyBlank(username, password, host, port)) {
        throw new IllegalArgumentException("username,password,port,host不能为空");
      }
    }

    @Override
    public void prepare() {
      this.init();
    }

    @Override
    public List<Configuration> split(int mandatoryNumber) {

      List<Configuration> configurations = new ArrayList<>(mandatoryNumber);

      for (int i = 0; i < mandatoryNumber; ++i) {
        configurations.add(this.originalConfig);
      }

      return configurations;
    }

    @Override
    public void post() {}

    @Override
    public void destroy() {}
  }
}
