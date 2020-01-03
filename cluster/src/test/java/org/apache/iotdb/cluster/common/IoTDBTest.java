/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;

/**
 * IoTDBTests are tests that need a IoTDB daemon to support the tests.
 */
public class IoTDBTest {

  private static IoTDB daemon = IoTDB.getInstance();
  private QueryProcessExecutor queryProcessExecutor;
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  public void setUp() throws StartupException, QueryProcessException {
    EnvironmentUtils.closeStatMonitor();
    daemon.active();
    EnvironmentUtils.envSetUp();
    queryProcessExecutor = new QueryProcessExecutor();
    prepareSchema();
    prepareData();
  }

  private void prepareSchema() throws QueryProcessException {
    for (int i = 0; i < 3; i++) {
      // storage groups that has timeseries schema locally
      setStorageGroup(getTestSg(i));
      for (int j = 0; j < 10; j++) {
        createTimeSeries(i, j);
      }
    }
    // storage groups that has timeseries schema remotely
    setStorageGroup(getTestSg(4));
    // storage groups that does not have timeseries schema remotely or locally
    setStorageGroup(getTestSg(5));
  }

  private void prepareData() throws QueryProcessException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(getTestSg(0));
    String[] measurements = new String[10];
    for (int i = 0; i < measurements.length; i++) {
      measurements[i] = getTestMeasurement(i);
    }
    TSDataType[] dataTypes = new TSDataType[10];
    for (int i = 0; i < dataTypes.length; i++) {
      dataTypes[i] = TSDataType.DOUBLE;
    }
    insertPlan.setMeasurements(measurements);
    insertPlan.setDataTypes(dataTypes);

    String[] values = new String[10];
    for (int i = 0; i < 100; i++) {
      insertPlan.setTime(i);
      for (int j = 0; j < values.length; j++) {
        values[j] = String.valueOf(i * 1.0);
      }
      insertPlan.setValues(values);
      queryProcessExecutor.insert(insertPlan);
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private void setStorageGroup(String storageGroupName) throws QueryProcessException {
    queryProcessExecutor.setStorageGroup(new SetStorageGroupPlan(new Path(storageGroupName)));
  }

  private void createTimeSeries(int sgNum, int seriesNum) throws QueryProcessException {
    MeasurementSchema schema = getTestSchema(sgNum, seriesNum);
    queryProcessExecutor.processNonQuery(new CreateTimeSeriesPlan(new Path(schema.getMeasurementId()),
        schema.getType(), schema.getEncodingType(), schema.getCompressor(), schema.getProps()));
  }

  protected QueryDataSet query(List<String> pathStrs, IExpression expression)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException, IOException, MetadataException {
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    QueryPlan queryPlan = new QueryPlan();
    queryPlan.setExpression(expression);
    List<Path> paths = new ArrayList<>();
    for (String pathStr : pathStrs) {
      paths.add(new Path(pathStr));
    }
    queryPlan.setDeduplicatedPaths(paths);
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : paths) {
      dataTypes.add(queryProcessExecutor.getSeriesType(path));
    }
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setExpression(expression);

    return queryProcessExecutor.processQuery(queryPlan, context);
  }

  protected String getTestSg(int i) {
    return "root.test" + i;
  }

  protected String getTestSeries(int sgNum, int seriesNum) {
    return getTestSg(sgNum) + "." + getTestMeasurement(seriesNum);
  }

  protected String getTestMeasurement(int seriesNum) {
    return "s" + seriesNum;
  }

  protected MeasurementSchema getTestSchema(int sgNum, int seriesNum) {
    String path = getTestSeries(sgNum, seriesNum);
    TSDataType dataType = TSDataType.DOUBLE;
    TSEncoding encoding = config.getDefaultDoubleEncoding();
    return new MeasurementSchema(path, dataType, encoding, CompressionType.UNCOMPRESSED,
        Collections.emptyMap());
  }
}
