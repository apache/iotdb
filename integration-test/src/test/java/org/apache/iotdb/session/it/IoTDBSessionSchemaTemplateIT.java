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

package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.template.TemplateNode;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionSchemaTemplateIT extends AbstractSchemaIT {

  private ISession session;

  public IoTDBSessionSchemaTemplateIT(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Before
  public void setUp() throws Exception {
    setUpEnvironmentBeforeMethod();
    EnvFactory.getEnv().initClusterEnvironment();
    session = EnvFactory.getEnv().getSessionConnection();
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Test
  public void testCreateFlatTemplate()
      throws IOException, StatementExecutionException, IoTDBConnectionException {
    String tempName = "flatTemplate";
    List<String> measurements = Arrays.asList("x", "y", "speed");
    List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.DOUBLE);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.GORILLA);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.LZ4);

    session.setStorageGroup("root.sg");

    session.createSchemaTemplate(tempName, measurements, dataTypes, encodings, compressors, true);
    session.setSchemaTemplate("flatTemplate", "root.sg.d0");
    try {
      session.setSchemaTemplate("flatTemplate", "root.sg.d0");
      fail();
    } catch (StatementExecutionException e) {
      assertEquals(
          TSStatusCode.METADATA_ERROR.getStatusCode() + ": Template already exists on root.sg.d0",
          e.getMessage());
    }
  }

  @Test
  public void testShowTemplates()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    session.setStorageGroup("root.sg");

    Template temp1 = getTemplate("template1");
    Template temp2 = getTemplate("template2");

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);
    session.createSchemaTemplate(temp2);

    assertEquals(
        new HashSet<>(Arrays.asList("template1", "template2")),
        new HashSet<>(session.showAllTemplates()));

    session.setSchemaTemplate("template1", "root.sg.v1");
    session.setSchemaTemplate("template1", "root.sg.v2");
    session.setSchemaTemplate("template1", "root.sg.v3");

    assertEquals(
        new HashSet<>(Arrays.asList("root.sg.v1", "root.sg.v2", "root.sg.v3")),
        new HashSet<>(session.showPathsTemplateSetOn("template1")));

    assertEquals(
        new HashSet<>(Collections.emptyList()),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    session.setSchemaTemplate("template2", "root.sg.v4");
    session.setSchemaTemplate("template2", "root.sg.v5");
    session.setSchemaTemplate("template2", "root.sg.v6");

    assertEquals(
        new HashSet<>(Arrays.asList("root.sg.v4", "root.sg.v5", "root.sg.v6")),
        new HashSet<>(session.showPathsTemplateSetOn("template2")));

    assertEquals(
        new HashSet<>(
            Arrays.asList(
                "root.sg.v1",
                "root.sg.v2",
                "root.sg.v3",
                "root.sg.v4",
                "root.sg.v5",
                "root.sg.v6")),
        new HashSet<>(session.showPathsTemplateSetOn("*")));

    session.insertRecord(
        "root.sg.v1.GPS",
        110L,
        Collections.singletonList("x"),
        Collections.singletonList(TSDataType.FLOAT),
        Collections.singletonList(1.0f));

    assertEquals(
        new HashSet<>(Collections.singletonList("root.sg.v1.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    session.insertRecord(
        "root.sg.v5.GPS",
        110L,
        Collections.singletonList("x"),
        Collections.singletonList(TSDataType.FLOAT),
        Collections.singletonList(1.0f));

    assertEquals(
        new HashSet<>(Collections.singletonList("root.sg.v1.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    assertEquals(
        new HashSet<>(Collections.singletonList("root.sg.v5.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template2")));
  }

  @Test
  public void testDropTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    session.setStorageGroup("root.sg");

    Template temp1 = getTemplate("template1");

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);

    assertEquals("[]", session.showPathsTemplateSetOn("template1").toString());

    try {
      session.createSchemaTemplate(temp1);
      fail();
    } catch (Exception e) {
      assertEquals(
          TSStatusCode.METADATA_ERROR.getStatusCode() + ": Duplicated template name: template1",
          e.getMessage());
    }

    session.dropSchemaTemplate("template1");
    session.createSchemaTemplate(temp1);

    session.setSchemaTemplate("template1", "root.sg.v1");

    try {
      session.dropSchemaTemplate("template1");
      fail();
    } catch (Exception e) {
      assertEquals(
          TSStatusCode.METADATA_ERROR.getStatusCode()
              + ": Template [template1] has been set on MTree, cannot be dropped now.",
          e.getMessage());
    }

    session.unsetSchemaTemplate("root.sg.v1", "template1");
    session.dropSchemaTemplate("template1");

    session.createSchemaTemplate(temp1);
  }

  private Template getTemplate(String name) throws StatementExecutionException {
    Template sessionTemplate = new Template(name, false);

    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    TemplateNode mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);

    sessionTemplate.addToTemplate(mNodeX);
    sessionTemplate.addToTemplate(mNodeY);

    return sessionTemplate;
  }

  @Test
  public void testBatchActivateTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    try {
      session.createTimeseriesUsingSchemaTemplate(Collections.singletonList(null));
      fail();
    } catch (StatementExecutionException e) {
      assertEquals("Given device path list should not be  or contains null.", e.getMessage());
    }

    try {
      session.createTimeseriesUsingSchemaTemplate(Collections.singletonList("root.db.d1"));
      fail();
    } catch (StatementExecutionException e) {
      assertTrue(e.getMessage().contains("Path [root.db.d1] has not been set any template"));
    }

    session.createDatabase("root.db");

    Template temp1 = getTemplate("template1");
    Template temp2 = getTemplate("template2");

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);
    session.createSchemaTemplate(temp2);

    assertEquals(
        new HashSet<>(Arrays.asList("template1", "template2")),
        new HashSet<>(session.showAllTemplates()));

    session.setSchemaTemplate("template1", "root.db.v1");
    session.setSchemaTemplate("template1", "root.db.v2");
    session.setSchemaTemplate("template1", "root.db.v3");

    assertEquals(
        new HashSet<>(Collections.emptyList()),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    session.setSchemaTemplate("template2", "root.db.v4");
    session.setSchemaTemplate("template2", "root.db.v5");
    session.setSchemaTemplate("template2", "root.db.v6");

    assertEquals(
        new HashSet<>(Arrays.asList("root.db.v4", "root.db.v5", "root.db.v6")),
        new HashSet<>(session.showPathsTemplateSetOn("template2")));

    session.createTimeseriesUsingSchemaTemplate(Collections.singletonList("root.db.v1.GPS"));

    assertEquals(
        new HashSet<>(Collections.singletonList("root.db.v1.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    session.createTimeseriesUsingSchemaTemplate(Collections.singletonList("root.db.v5.GPS"));

    assertEquals(
        new HashSet<>(Collections.singletonList("root.db.v1.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    assertEquals(
        new HashSet<>(Collections.singletonList("root.db.v5.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template2")));

    session.createTimeseriesUsingSchemaTemplate(
        Arrays.asList("root.db.v2.GPS", "root.db.v3.GPS", "root.db.v4.GPS", "root.db.v6.GPS"));

    assertEquals(
        new HashSet<>(Arrays.asList("root.db.v1.GPS", "root.db.v2.GPS", "root.db.v3.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    assertEquals(
        new HashSet<>(Arrays.asList("root.db.v4.GPS", "root.db.v5.GPS", "root.db.v6.GPS")),
        new HashSet<>(session.showPathsTemplateUsingOn("template2")));
  }

  @Test
  public void testInsertRecordsWithTemplate() throws Exception {
    session.createDatabase("root.db");

    Template temp1 = getTemplate("template1");
    session.createSchemaTemplate(temp1);

    session.setSchemaTemplate("template1", "root.db.v1");

    List<String> devices = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<String> measurements = Arrays.asList(new String[] {"x", "y"});
    List<Long> times = new ArrayList<>();
    List<List<String>> values = new ArrayList<>();
    List<String> value = Arrays.asList(new String[] {"1.23", "2.34"});
    for (int i = 0; i < 101; i++) {
      devices.add("root.db.v1.d" + i);
      measurementsList.add(measurements);
      times.add(12345L + i);
      values.add(value);
    }

    session.insertRecords(devices, times, measurementsList, values);
    SessionDataSet dataSet;
    RowRecord row;
    for (int i = 0; i < 10; i++) {
      dataSet =
          session.executeQueryStatement(
              String.format("SELECT * from root.db.v1.d%d", (int) (Math.random() * 100)));
      while (dataSet.hasNext()) {
        row = dataSet.next();
        Assert.assertEquals("1.23", row.getFields().get(0).toString());
        Assert.assertEquals("2.34", row.getFields().get(1).toString());
      }
    }
  }

  @Test
  public void testHybridAutoCreateSchema()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    session.createDatabase("root.db");

    Template temp1 = getTemplate("template1");
    Template temp2 = getTemplate("template2");

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);
    session.createSchemaTemplate(temp2);

    session.setSchemaTemplate("template1", "root.db.v1");

    session.createTimeseriesUsingSchemaTemplate(Collections.singletonList("root.db.v1.d1"));

    session.setSchemaTemplate("template2", "root.db.v4");

    List<String> deviceIds =
        Arrays.asList("root.db.v1.d1", "root.db.v1.d2", "root.db.v2.d1", "root.db.v4.d1");
    List<Long> timestamps = Arrays.asList(1L, 1L, 1L, 1L);
    List<String> measurements = Arrays.asList("x", "y", "z");
    List<List<String>> allMeasurements =
        Arrays.asList(measurements, measurements, measurements, measurements);
    List<TSDataType> tsDataTypes =
        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.TEXT);
    List<List<TSDataType>> allTsDataTypes =
        Arrays.asList(tsDataTypes, tsDataTypes, tsDataTypes, tsDataTypes);
    List<Object> values = Arrays.asList(1f, 2f, "3");
    List<List<Object>> allValues = Arrays.asList(values, values, values, values);

    session.insertRecords(deviceIds, timestamps, allMeasurements, allTsDataTypes, allValues);

    Set<String> expectedSeries =
        new HashSet<>(
            Arrays.asList(
                "root.db.v1.d1.x",
                "root.db.v1.d1.y",
                "root.db.v1.d1.z",
                "root.db.v1.d2.x",
                "root.db.v1.d2.y",
                "root.db.v1.d2.z",
                "root.db.v2.d1.x",
                "root.db.v2.d1.y",
                "root.db.v2.d1.z",
                "root.db.v4.d1.x",
                "root.db.v4.d1.y",
                "root.db.v4.d1.z"));

    try (SessionDataSet dataSet = session.executeQueryStatement("show timeseries")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        Assert.assertTrue(expectedSeries.contains(iterator.getString(1)));
        expectedSeries.remove(iterator.getString(1));
      }
    }

    Assert.assertTrue(expectedSeries.isEmpty());
  }

  @Test
  public void testHybridAutoExtendSchemaTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    session.createDatabase("root.db");

    Template temp1 = getTemplate("template1");
    Template temp2 = new Template("template2", false);

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);
    session.createSchemaTemplate(temp2);

    session.setSchemaTemplate("template1", "root.db.v1");

    session.createTimeseriesUsingSchemaTemplate(Collections.singletonList("root.db.v1.d1"));

    session.setSchemaTemplate("template2", "root.db.v4");

    List<String> deviceIds =
        Arrays.asList(
            "root.db.v1.d1", "root.db.v1.d2", "root.db.v2.d1", "root.db.v4.d1", "root.db.v4.d2");
    List<Long> timestamps = Arrays.asList(1L, 1L, 1L, 1L, 1L);
    List<String> measurements = Arrays.asList("x", "y", "z");
    List<List<String>> allMeasurements =
        Arrays.asList(measurements, measurements, measurements, measurements, measurements);
    List<TSDataType> tsDataTypes =
        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.TEXT);
    List<List<TSDataType>> allTsDataTypes =
        Arrays.asList(tsDataTypes, tsDataTypes, tsDataTypes, tsDataTypes, tsDataTypes);
    List<Object> values = Arrays.asList(1f, 2f, "3");
    List<List<Object>> allValues = Arrays.asList(values, values, values, values, values);

    session.insertRecords(deviceIds, timestamps, allMeasurements, allTsDataTypes, allValues);

    Set<String> expectedSeries =
        new HashSet<>(
            Arrays.asList(
                "root.db.v1.d1.x",
                "root.db.v1.d1.y",
                "root.db.v1.d1.z",
                "root.db.v1.d2.x",
                "root.db.v1.d2.y",
                "root.db.v1.d2.z",
                "root.db.v2.d1.x",
                "root.db.v2.d1.y",
                "root.db.v2.d1.z",
                "root.db.v4.d1.x",
                "root.db.v4.d1.y",
                "root.db.v4.d1.z",
                "root.db.v4.d2.x",
                "root.db.v4.d2.y",
                "root.db.v4.d2.z"));

    try (SessionDataSet dataSet = session.executeQueryStatement("show timeseries")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        Assert.assertTrue(expectedSeries.contains(iterator.getString(1)));
        expectedSeries.remove(iterator.getString(1));
      }
    }

    Assert.assertTrue(expectedSeries.isEmpty());

    measurements = Arrays.asList("a", "b", "c");

    try {
      session.insertRecord(
          "root.db.v4.d1",
          1L,
          measurements,
          Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.TEXT),
          Arrays.asList(1f, 2f, "3"));
      session.insertRecord(
          "root.db.v4.d2",
          1L,
          measurements,
          Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT32),
          Arrays.asList(1d, 2d, 3));
      Assert.fail();
    } catch (StatementExecutionException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "data type of root.db.v4.d2.a is not consistent, registered type FLOAT, inserting type DOUBLE"));
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "data type of root.db.v4.d2.b is not consistent, registered type FLOAT, inserting type DOUBLE"));
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "data type of root.db.v4.d2.c is not consistent, registered type TEXT, inserting type INT32"));
    }
  }
}
