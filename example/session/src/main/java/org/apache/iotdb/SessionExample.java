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

package org.apache.iotdb;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@SuppressWarnings({"squid:S106", "squid:S1144", "squid:S125"})
public class SessionExample {

  private static Session session;
  private static Session sessionEnableRedirect;
  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
  private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
  private static final String ROOT_SG1_D1_S4 = "root.sg1.d1.s4";
  private static final String ROOT_SG1_D1_S5 = "root.sg1.d1.s5";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final String ROOT_SG1 = "root.sg1";
  private static final String LOCAL_HOST = "127.0.0.1";
  public static final String SELECT_D1 = "select * from root.sg1.d1";

  private static Random random = new Random();

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    try {
      session.createDatabase("root.sg1");
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        throw e;
      }
    }

    //     createTemplate();
    createTimeseries();
    createMultiTimeseries();
    insertRecord();
    insertTablet();
    //    insertTabletWithNullValues();
    //    insertTablets();
    //    insertRecords();
    //    insertText();
    //    selectInto();
    //    createAndDropContinuousQueries();
    //    nonQuery();
    query();
    //    queryWithTimeout();
    rawDataQuery();
    lastDataQuery();
    aggregationQuery();
    groupByQuery();
    //    queryByIterator();
    //    deleteData();
    //    deleteTimeseries();
    //    setTimeout();

    sessionEnableRedirect = new Session(LOCAL_HOST, 6667, "root", "root");
    sessionEnableRedirect.setEnableQueryRedirection(true);
    sessionEnableRedirect.open(false);

    // set session fetchSize
    sessionEnableRedirect.setFetchSize(10000);

    fastLastDataQueryForOneDevice();
    insertRecord4Redirect();
    query4Redirect();
    sessionEnableRedirect.close();
    session.close();
  }

  private static void createAndDropContinuousQueries()
      throws StatementExecutionException, IoTDBConnectionException {
    session.executeNonQueryStatement(
        "CREATE CONTINUOUS QUERY cq1 "
            + "BEGIN SELECT max_value(s1) INTO temperature_max FROM root.sg1.* "
            + "GROUP BY time(10s) END");
    session.executeNonQueryStatement(
        "CREATE CONTINUOUS QUERY cq2 "
            + "BEGIN SELECT count(s2) INTO temperature_cnt FROM root.sg1.* "
            + "GROUP BY time(10s), level=1 END");
    session.executeNonQueryStatement(
        "CREATE CONTINUOUS QUERY cq3 "
            + "RESAMPLE EVERY 20s FOR 20s "
            + "BEGIN SELECT avg(s3) INTO temperature_avg FROM root.sg1.* "
            + "GROUP BY time(10s), level=1 END");
    session.executeNonQueryStatement("DROP CONTINUOUS QUERY cq1");
    session.executeNonQueryStatement("DROP CONTINUOUS QUERY cq2");
    session.executeNonQueryStatement("DROP CONTINUOUS QUERY cq3");
  }

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
      session.createTimeseries(
          ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S2)) {
      session.createTimeseries(
          ROOT_SG1_D1_S2, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S3)) {
      session.createTimeseries(
          ROOT_SG1_D1_S3, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }

    // create timeseries with tags and attributes
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S4)) {
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "v1");
      Map<String, String> attributes = new HashMap<>();
      attributes.put("description", "v1");
      session.createTimeseries(
          ROOT_SG1_D1_S4,
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY,
          null,
          tags,
          attributes,
          "temperature");
    }

    // create timeseries with SDT property, SDT will take place when flushing
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S5)) {
      // COMPDEV is required
      // COMPMAXTIME and COMPMINTIME are optional and their unit is ms
      Map<String, String> props = new HashMap<>();
      props.put("LOSS", "sdt");
      props.put("COMPDEV", "0.01");
      props.put("COMPMINTIME", "2");
      props.put("COMPMAXTIME", "10");
      session.createTimeseries(
          ROOT_SG1_D1_S5,
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY,
          props,
          null,
          null,
          null);
    }
  }

  private static void createMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists("root.sg1.d2.s1")
        && !session.checkTimeseriesExists("root.sg1.d2.s2")) {
      List<String> paths = new ArrayList<>();
      paths.add("root.sg1.d2.s1");
      paths.add("root.sg1.d2.s2");
      List<TSDataType> tsDataTypes = new ArrayList<>();
      tsDataTypes.add(TSDataType.INT64);
      tsDataTypes.add(TSDataType.INT64);
      List<TSEncoding> tsEncodings = new ArrayList<>();
      tsEncodings.add(TSEncoding.RLE);
      tsEncodings.add(TSEncoding.RLE);
      List<CompressionType> compressionTypes = new ArrayList<>();
      compressionTypes.add(CompressionType.SNAPPY);
      compressionTypes.add(CompressionType.SNAPPY);

      List<Map<String, String>> tagsList = new ArrayList<>();
      Map<String, String> tags = new HashMap<>();
      tags.put("unit", "kg");
      tagsList.add(tags);
      tagsList.add(tags);

      List<Map<String, String>> attributesList = new ArrayList<>();
      Map<String, String> attributes = new HashMap<>();
      attributes.put("minValue", "1");
      attributes.put("maxValue", "100");
      attributesList.add(attributes);
      attributesList.add(attributes);

      List<String> alias = new ArrayList<>();
      alias.add("weight1");
      alias.add("weight2");

      session.createMultiTimeseries(
          paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList, attributesList, alias);
    }
  }

  private static void createTemplate()
      throws IoTDBConnectionException, StatementExecutionException, IOException {

    Template template = new Template("template1", false);
    MeasurementNode mNodeS1 =
        new MeasurementNode("s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS2 =
        new MeasurementNode("s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS3 =
        new MeasurementNode("s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    template.addToTemplate(mNodeS1);
    template.addToTemplate(mNodeS2);
    template.addToTemplate(mNodeS3);

    session.createSchemaTemplate(template);
    session.setSchemaTemplate("template1", "root.sg1");
  }

  private static void insertRecord() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  private static void insertRecord4Redirect()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < 6; i++) {
      for (int j = 0; j < 2; j++) {
        String deviceId = "root.redirect" + i + ".d" + j;
        List<String> measurements = new ArrayList<>();
        measurements.add("s1");
        measurements.add("s2");
        measurements.add("s3");
        List<TSDataType> types = new ArrayList<>();
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);

        for (long time = 0; time < 5; time++) {
          List<Object> values = new ArrayList<>();
          values.add(1L + time);
          values.add(2L + time);
          values.add(3L + time);
          session.insertRecord(deviceId, time, measurements, types, values);
        }
      }
    }
  }

  private static void insertStrRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    for (long time = 0; time < 10; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }
  }

  private static void insertRecordInObject()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      session.insertRecord(deviceId, time, measurements, types, 1L, 1L, 1L);
    }
  }

  private static void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        typesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = random.nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertTabletWithNullValues()
      throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1,   s2,   s3
     * 1,   null, 1,    1
     * 2,   2,    null, 2
     * 3,   3,    3,    null
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    // Method 1 to add tablet data
    insertTablet1(schemaList, tablet);

    // Method 2 to add tablet data
    insertTablet2(schemaList, tablet);
  }

  private static void insertTablet1(List<IMeasurementSchema> schemaList, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    tablet.initBitMaps();

    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = random.nextLong();
        // mark null value
        if (row % 3 == s) {
          tablet.bitMaps[s].mark((int) row);
        }
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertTablet2(List<IMeasurementSchema> schemaList, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    BitMap[] bitMaps = new BitMap[schemaList.size()];
    for (int s = 0; s < 3; s++) {
      bitMaps[s] = new BitMap(tablet.getMaxRowNumber());
    }
    tablet.bitMaps = bitMaps;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        // mark null value
        if (row % 3 == i) {
          bitMaps[i].mark(row);
        }
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertTablets() throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(ROOT_SG1_D1, schemaList, 100);
    Tablet tablet2 = new Tablet("root.sg1.d2", schemaList, 100);
    Tablet tablet3 = new Tablet("root.sg1.d3", schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(ROOT_SG1_D1, tablet1);
    tabletMap.put("root.sg1.d2", tablet2);
    tabletMap.put("root.sg1.d3", tablet3);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = random.nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps1 = tablet1.timestamps;
    Object[] values1 = tablet1.values;
    long[] timestamps2 = tablet2.timestamps;
    Object[] values2 = tablet2.values;
    long[] timestamps3 = tablet3.timestamps;
    Object[] values3 = tablet3.values;

    for (long time = 0; time < 100; time++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      timestamps3[row3] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
        long[] sensor3 = (long[]) values3[i];
        sensor3[row3] = i;
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);

        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }
  }

  /**
   * This example shows how to insert data of TSDataType.TEXT. You can use the session interface to
   * write data of String type or Binary type.
   */
  private static void insertText() throws IoTDBConnectionException, StatementExecutionException {
    String device = "root.sg1.text";
    // the first data is String type and the second data is Binary type
    List<Object> datas = Arrays.asList("String", new Binary("Binary", TSFileConfig.STRING_CHARSET));
    // insertRecord example
    for (int i = 0; i < datas.size(); i++) {
      // write data of String type or Binary type
      session.insertRecord(
          device,
          i,
          Collections.singletonList("s1"),
          Collections.singletonList(TSDataType.TEXT),
          datas.get(i));
    }

    // insertTablet example
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s2", TSDataType.TEXT));
    Tablet tablet = new Tablet(device, schemaList, 100);
    for (int i = 0; i < datas.size(); i++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, i);
      //  write data of String type or Binary type
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, datas.get(i));
    }
    session.insertTablet(tablet);
    try (SessionDataSet dataSet = session.executeQueryStatement("select s1, s2 from " + device)) {
      System.out.println(dataSet.getColumnNames());
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }
    }
  }

  private static void selectInto() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "select s1, s2, s3 into into_s1, into_s2, into_s3 from root.sg1.d1");

    try (SessionDataSet dataSet =
        session.executeQueryStatement("select into_s1, into_s2, into_s3 from root.sg1.d1")) {
      System.out.println(dataSet.getColumnNames());
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }
    }
  }

  private static void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path = ROOT_SG1_D1_S1;
    long deleteTime = 99;
    session.deleteData(path, deleteTime);
  }

  private static void deleteTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    session.deleteTimeseries(paths);
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet dataSet = session.executeQueryStatement(SELECT_D1)) {
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }
    }
  }

  private static void query4Redirect()
      throws IoTDBConnectionException, StatementExecutionException {
    String selectPrefix = "select * from root.redirect";
    for (int i = 0; i < 6; i++) {
      try (SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(selectPrefix + i + ".d1")) {

        System.out.println(dataSet.getColumnNames());
        dataSet.setFetchSize(1024); // default is 10000
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }
    }

    for (int i = 0; i < 6; i++) {
      try (SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(
              selectPrefix + i + ".d1 where time >= 1 and time < 10")) {

        System.out.println(dataSet.getColumnNames());
        dataSet.setFetchSize(1024); // default is 10000
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }
    }

    for (int i = 0; i < 6; i++) {
      try (SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(
              selectPrefix + i + ".d1 where time >= 1 and time < 10 align by device")) {

        System.out.println(dataSet.getColumnNames());
        dataSet.setFetchSize(1024); // default is 10000
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }
    }

    for (int i = 0; i < 6; i++) {
      try (SessionDataSet dataSet =
          sessionEnableRedirect.executeQueryStatement(
              selectPrefix
                  + i
                  + ".d1 where time >= 1 and time < 10 and root.redirect"
                  + i
                  + ".d1.s1 > 1")) {
        System.out.println(dataSet.getColumnNames());
        dataSet.setFetchSize(1024); // default is 10000
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }
    }
  }

  private static void queryWithTimeout()
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet dataSet = session.executeQueryStatement(SELECT_D1, 2000)) {
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }
    }
  }

  private static void rawDataQuery() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    long startTime = 10L;
    long endTime = 200L;
    long timeOut = 60000;

    try (SessionDataSet dataSet = session.executeRawDataQuery(paths, startTime, endTime, timeOut)) {

      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024);
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }
    }
  }

  private static void lastDataQuery() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    try (SessionDataSet sessionDataSet = session.executeLastDataQuery(paths, 3, 60000)) {
      System.out.println(sessionDataSet.getColumnNames());
      sessionDataSet.setFetchSize(1024);
      while (sessionDataSet.hasNext()) {
        System.out.println(sessionDataSet.next());
      }
    }
  }

  private static void fastLastDataQueryForOneDevice()
      throws IoTDBConnectionException, StatementExecutionException {
    System.out.println("-------fastLastQuery------");
    List<String> paths = new ArrayList<>();
    paths.add("s1");
    paths.add("s2");
    paths.add("s3");
    try (SessionDataSet sessionDataSet =
        sessionEnableRedirect.executeLastDataQueryForOneDevice(
            ROOT_SG1, ROOT_SG1_D1, paths, true)) {
      System.out.println(sessionDataSet.getColumnNames());
      sessionDataSet.setFetchSize(1024);
      while (sessionDataSet.hasNext()) {
        System.out.println(sessionDataSet.next());
      }
    }
  }

  private static void aggregationQuery()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);

    List<TAggregationType> aggregations = new ArrayList<>();
    aggregations.add(TAggregationType.COUNT);
    aggregations.add(TAggregationType.SUM);
    aggregations.add(TAggregationType.MAX_VALUE);
    try (SessionDataSet sessionDataSet = session.executeAggregationQuery(paths, aggregations)) {
      System.out.println(sessionDataSet.getColumnNames());
      sessionDataSet.setFetchSize(1024);
      while (sessionDataSet.hasNext()) {
        System.out.println(sessionDataSet.next());
      }
    }
  }

  private static void groupByQuery() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);

    List<TAggregationType> aggregations = new ArrayList<>();
    aggregations.add(TAggregationType.COUNT);
    aggregations.add(TAggregationType.SUM);
    aggregations.add(TAggregationType.MAX_VALUE);
    try (SessionDataSet sessionDataSet =
        session.executeAggregationQuery(paths, aggregations, 0, 100, 10, 20)) {
      System.out.println(sessionDataSet.getColumnNames());
      sessionDataSet.setFetchSize(1024);
      while (sessionDataSet.hasNext()) {
        System.out.println(sessionDataSet.next());
      }
    }
  }

  private static void queryByIterator()
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet dataSet = session.executeQueryStatement(SELECT_D1)) {

      DataIterator iterator = dataSet.iterator();
      System.out.println(dataSet.getColumnNames());
      dataSet.setFetchSize(1024); // default is 10000
      while (iterator.next()) {
        StringBuilder builder = new StringBuilder();
        // get time
        builder.append(iterator.getLong(1)).append(",");
        // get second column
        if (!iterator.isNull(2)) {
          builder.append(iterator.getLong(2)).append(",");
        } else {
          builder.append("null").append(",");
        }

        // get third column
        if (!iterator.isNull(ROOT_SG1_D1_S2)) {
          builder.append(iterator.getLong(ROOT_SG1_D1_S2)).append(",");
        } else {
          builder.append("null").append(",");
        }

        // get forth column
        if (!iterator.isNull(4)) {
          builder.append(iterator.getLong(4)).append(",");
        } else {
          builder.append("null").append(",");
        }

        // get fifth column
        if (!iterator.isNull(ROOT_SG1_D1_S4)) {
          builder.append(iterator.getObject(ROOT_SG1_D1_S4));
        } else {
          builder.append("null");
        }

        System.out.println(builder);
      }
    }
  }

  private static void nonQuery() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1)");
  }

  private static void setTimeout() throws IoTDBConnectionException {
    try (Session tempSession = new Session(LOCAL_HOST, 6667, "root", "root", 10000, 20000)) {
      tempSession.setQueryTimeout(60000);
    }
  }
}
