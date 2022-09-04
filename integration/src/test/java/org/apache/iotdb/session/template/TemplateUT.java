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
package org.apache.iotdb.session.template;

import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class TemplateUT {

  private Session session;

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
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

    session.createSchemaTemplate(tempName, measurements, dataTypes, encodings, compressors, true);
    session.setSchemaTemplate("flatTemplate", "root.sg.d0");
    try {
      session.setSchemaTemplate("flatTemplate", "root.sg.d0");
      fail();
    } catch (StatementExecutionException e) {
      assertEquals("303: Template already exists on root.sg.d0", e.getMessage());
    }
  }

  @Test
  public void testTemplateTree()
      throws IOException, MetadataException, StatementExecutionException, IoTDBConnectionException {
    Template sessionTemplate = new Template("treeTemplate", true);
    TemplateNode iNodeGPS = new InternalNode("GPS", false);
    TemplateNode iNodeV = new InternalNode("vehicle", true);
    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    TemplateNode mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    iNodeGPS.addChild(mNodeX);
    iNodeGPS.addChild(mNodeY);
    iNodeV.addChild(mNodeX);
    iNodeV.addChild(mNodeY);
    iNodeV.addChild(iNodeGPS);
    sessionTemplate.addToTemplate(iNodeGPS);
    sessionTemplate.addToTemplate(iNodeV);
    sessionTemplate.addToTemplate(mNodeX);
    sessionTemplate.addToTemplate(mNodeY);

    sessionTemplate.serialize(stream);
    ByteBuffer buffer = ByteBuffer.wrap(stream.toByteArray());

    CreateTemplatePlan plan = CreateTemplatePlan.deserializeFromReq(buffer);
    assertEquals("treeTemplate", plan.getName());
    assertEquals(
        "[[vehicle.GPS.y], [vehicle.GPS.x], [GPS.y], [GPS.x], [y, x], [vehicle.y, vehicle.x]]",
        plan.getMeasurements().toString());

    session.createSchemaTemplate(sessionTemplate);
    session.setSchemaTemplate("treeTemplate", "root.sg.d0");
    try {
      session.setSchemaTemplate("treeTemplate", "root.sg.d0");
      fail();
    } catch (StatementExecutionException e) {
      assertEquals("303: Template already exists on root.sg.d0", e.getMessage());
    }
  }

  @Test
  public void testShowTemplates()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
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

    session.setStorageGroup("root.test.sgt");
    session.setSchemaTemplate("template1", "root.test.sgt");
    assertEquals(
        new HashSet<>(Arrays.asList("root.test.sgt", "root.sg.v1", "root.sg.v2", "root.sg.v3")),
        new HashSet<>(session.showPathsTemplateSetOn("template1")));

    assertEquals(
        new HashSet<>(Arrays.asList()),
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
                "root.sg.v6",
                "root.test.sgt")),
        new HashSet<>(session.showPathsTemplateSetOn("*")));

    session.insertRecord(
        "root.sg.v1.GPS",
        110L,
        Arrays.asList("x"),
        Arrays.asList(TSDataType.FLOAT),
        Arrays.asList(1.0f));

    assertEquals(
        new HashSet<>(Arrays.asList("root.sg.v1")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));

    session.insertRecord(
        "root.sg.v5.GPS",
        110L,
        Arrays.asList("x"),
        Arrays.asList(TSDataType.FLOAT),
        Arrays.asList(1.0f));

    assertEquals(
        new HashSet<>(Arrays.asList("root.sg.v1", "root.sg.v5")),
        new HashSet<>(session.showPathsTemplateUsingOn("*")));

    session.insertRecord(
        "root.test.sgt.GPS",
        110L,
        Arrays.asList("x"),
        Arrays.asList(TSDataType.FLOAT),
        Arrays.asList(1.0f));

    assertEquals(
        new HashSet<>(Arrays.asList("root.sg.v1", "root.sg.v5", "root.test.sgt")),
        new HashSet<>(session.showPathsTemplateUsingOn("*")));

    assertEquals(
        new HashSet<>(Arrays.asList("root.test.sgt", "root.sg.v1")),
        new HashSet<>(session.showPathsTemplateUsingOn("template1")));
  }

  @Test
  public void testDropTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    Template temp1 = getTemplate("template1");

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);

    assertEquals("[]", session.showPathsTemplateSetOn("template1").toString());

    try {
      session.createSchemaTemplate(temp1);
      fail();
    } catch (Exception e) {
      assertEquals("303: Duplicated template name: template1", e.getMessage());
    }

    session.dropSchemaTemplate("template1");
    session.createSchemaTemplate(temp1);

    session.setSchemaTemplate("template1", "root.sg.v1");

    try {
      session.dropSchemaTemplate("template1");
      fail();
    } catch (Exception e) {
      assertEquals(
          "303: Template [template1] has been set on MTree, cannot be dropped now.",
          e.getMessage());
    }

    session.unsetSchemaTemplate("root.sg.v1", "template1");
    session.dropSchemaTemplate("template1");

    session.createSchemaTemplate(temp1);
  }

  @Test
  public void testUpdateTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    Template temp1 = getTemplate("template1");
    session.createSchemaTemplate(temp1);

    session.deleteNodeInTemplate("template1", "x");
    session.deleteNodeInTemplate("template1", "y");

    session.setSchemaTemplate("template1", "root.sg.v1");

    session.addAlignedMeasurementsInTemplate(
        "template1",
        Collections.singletonList("append"),
        Collections.singletonList(TSDataType.INT64),
        Collections.singletonList(TSEncoding.RLE),
        Collections.singletonList(CompressionType.UNCOMPRESSED));

    try {
      session.deleteNodeInTemplate("template1", "append");
      fail();
    } catch (StatementExecutionException e) {
      assertEquals(
          "303: Template [template1] cannot be pruned since had been set before.", e.getMessage());
    }

    session.close();

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    session.insertAlignedRecord(
        "root.sg.v1",
        110L,
        Collections.singletonList("x"),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList("valueX"));

    try {
      session.insertAlignedRecord(
          "root.sg.v1",
          110L,
          Collections.singletonList("append"),
          Collections.singletonList(TSDataType.TEXT),
          Collections.singletonList("aaa"));
      fail();
    } catch (StatementExecutionException e) {
      assertEquals(313, e.getStatusCode());
    }

    try {
      session.addAlignedMeasurementsInTemplate(
          "template1",
          Collections.singletonList("append"),
          Collections.singletonList(TSDataType.TEXT),
          Collections.singletonList(TSEncoding.PLAIN),
          Collections.singletonList(CompressionType.UNCOMPRESSED));
      fail();
    } catch (StatementExecutionException e) {
      assertEquals("315: Path duplicated: append is not a legal path", e.getMessage());
    }

    try {
      session.addAlignedMeasurementsInTemplate(
          "template1",
          Collections.singletonList("x"),
          Collections.singletonList(TSDataType.TEXT),
          Collections.singletonList(TSEncoding.PLAIN),
          Collections.singletonList(CompressionType.UNCOMPRESSED));
      fail();
    } catch (StatementExecutionException e) {
      assertEquals(
          "303: Template [template1] cannot be appended for overlapping of new measurement and MTree",
          e.getMessage());
    }

    try {
      session.addUnalignedMeasurementsInTemplate(
          "template1",
          Collections.singletonList("y"),
          Collections.singletonList(TSDataType.TEXT),
          Collections.singletonList(TSEncoding.PLAIN),
          Collections.singletonList(CompressionType.UNCOMPRESSED));
      fail();
    } catch (StatementExecutionException e) {
      assertEquals(
          "315: y is not a legal path, because path already exists and aligned", e.getMessage());
    }

    session.addAlignedMeasurementsInTemplate(
        "template1",
        Collections.singletonList("y"),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList(TSEncoding.PLAIN),
        Collections.singletonList(CompressionType.UNCOMPRESSED));

    session.insertAlignedRecord(
        "root.sg.v1",
        110L,
        Collections.singletonList("y"),
        Collections.singletonList(TSDataType.TEXT),
        Collections.singletonList("valueY"));

    session.insertAlignedRecord(
        "root.sg.v1",
        110L,
        Collections.singletonList("append"),
        Collections.singletonList(TSDataType.INT64),
        Collections.singletonList(12345L));

    SessionDataSet res =
        session.executeRawDataQuery(Collections.singletonList("root.sg.v1.*"), 0L, 999L);
    while (res.hasNext()) {
      RowRecord rec = res.next();
      // correspond to x, y, append
      assertEquals(3, rec.getFields().size());
    }

    session.insertAlignedRecord(
        "root.sg.v1.d0",
        110L,
        Collections.singletonList("append"),
        Collections.singletonList(TSDataType.INT64),
        Collections.singletonList(12345L));

    res = session.executeRawDataQuery(Collections.singletonList("root.sg.v1.d0.*"), 0L, 999L);
    while (res.hasNext()) {
      RowRecord rec = res.next();
      // x is not inside template
      assertEquals(2, rec.getFields().size());
    }
  }

  @Test
  public void testUnsetTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    Template temp1 = getTemplate("template1");
    session.createSchemaTemplate(temp1);
    session.setSchemaTemplate("template1", "root.sg.v1");
    session.createTimeseriesOfTemplateOnPath("root.sg.v1.l1.d1");

    try {
      session.executeNonQueryStatement("delete timeseries root.sg.v1.l1.d1.x");
      fail();
    } catch (BatchExecutionException e) {
    }

    // wildcard usage on prefix
    session.deactivateTemplateOn("template1", "root");
    assertEquals(1, session.showPathsTemplateUsingOn("template1").size());

    session.deactivateTemplateOn("template1", "root.*");
    assertEquals(1, session.showPathsTemplateUsingOn("template1").size());

    session.deactivateTemplateOn("template1", "root.**");
    assertEquals(0, session.showPathsTemplateUsingOn("template1").size());

    session.createTimeseriesOfTemplateOnPath("root.sg.v1.l1.d1");
    assertEquals(1, session.showPathsTemplateUsingOn("template1").size());

    session.deactivateTemplateOn("template1", "root.sg.v1.l1.d1.*");
    assertEquals(1, session.showPathsTemplateUsingOn("template1").size());

    session.deactivateTemplateOn("template1", "root.sg.v1.l1.d1.**");
    assertEquals(1, session.showPathsTemplateUsingOn("template1").size());

    session.deactivateTemplateOn("template1", "root.sg.v1.l1.d1");
    assertEquals(0, session.showPathsTemplateUsingOn("template1").size());

    session.deactivateTemplateOn("template1", "root.sg.v1.l1.d1");
    session.unsetSchemaTemplate("root.sg.v1", "template1");
    assertEquals(0, session.showPathsTemplateSetOn("template1").size());
    session.dropSchemaTemplate("template1");
    assertEquals(0, session.showAllTemplates().size());

    // data delete and rewrite
    session.createSchemaTemplate(temp1);
    session.setSchemaTemplate("template1", "root.sg.v1");
    session.createTimeseriesOfTemplateOnPath("root.sg.v1");
    session.insertAlignedRecord(
        "root.sg.v1", 11L, Collections.singletonList("x"), Collections.singletonList("1.1"));
    SessionDataSet sds = session.executeQueryStatement("select * from root.sg.v1");
    int cnt = 0;
    while (sds.hasNext()) {
      cnt++;
      sds.next();
    }
    assertEquals(1, cnt);
    session.deactivateTemplateOn("template1", "root.**");

    sds = session.executeQueryStatement("select * from root.sg.v1");
    cnt = 0;
    while (sds.hasNext()) {
      cnt++;
      sds.next();
    }
    assertEquals(0, cnt);

    try {
      session.insertAlignedRecord(
          "root.sg.v1", 999L, Collections.singletonList("x"), Collections.singletonList("abcd"));
      fail();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      session.insertRecord(
          "root.sg.v1", 999L, Collections.singletonList("x"), Collections.singletonList("abcd"));
      fail();
    } catch (Exception e) {
      e.printStackTrace();
    }

    session.deactivateTemplateOn("template1", "root.**");
    session.unsetSchemaTemplate("root.sg.v1", "template1");
    session.insertAlignedRecord(
        "root.sg.v1", 911L, Collections.singletonList("x"), Collections.singletonList("abcd"));

    sds = session.executeQueryStatement("select * from root.sg.v1");
    cnt = 0;
    while (sds.hasNext()) {
      cnt++;
      assertEquals("abcd", sds.next().getFields().get(0).toString());
    }
    assertEquals(1, cnt);
  }

  @Test
  public void testTemplatePrivilege() throws Exception {
    session.executeNonQueryStatement("CREATE USER tpl_user 'tpl_pwd'");
    Session nSession = new Session("127.0.0.1", 6667, "tpl_user", "tpl_pwd", ZoneId.of("+05:00"));
    nSession.open();
    try {
      nSession.createSchemaTemplate(getTemplate("t1"));
    } catch (Exception e) {
      assertEquals(
          "602: No permissions for this operation, please add privilege "
              + PrivilegeType.values()[
                  AuthorityChecker.translateToPermissionId(Operator.OperatorType.CREATE_TEMPLATE)],
          e.getMessage());
    }

    session.executeNonQueryStatement(
        "grant user tpl_user privileges UPDATE_TEMPLATE on root.irrelevant");
    nSession.createSchemaTemplate(getTemplate("t1"));
    nSession.createSchemaTemplate(getTemplate("t2"));
    assertEquals(2, nSession.showAllTemplates().size());
    nSession.dropSchemaTemplate("t2");
    assertEquals(1, nSession.showAllTemplates().size());

    try {
      nSession.setSchemaTemplate("t1", "root.sg2.d1");
    } catch (Exception e) {
      assertEquals(
          "602: No permissions for this operation, please add privilege "
              + PrivilegeType.values()[
                  AuthorityChecker.translateToPermissionId(Operator.OperatorType.SET_TEMPLATE)],
          e.getMessage());
    }

    session.executeNonQueryStatement("grant user tpl_user privileges APPLY_TEMPLATE on root.sg1");
    try {
      nSession.setSchemaTemplate("t1", "root.sg2.d1");
    } catch (Exception e) {
      assertEquals(
          "602: No permissions for this operation, please add privilege "
              + PrivilegeType.values()[
                  AuthorityChecker.translateToPermissionId(Operator.OperatorType.SET_TEMPLATE)],
          e.getMessage());
    }

    session.executeNonQueryStatement("grant user tpl_user privileges APPLY_TEMPLATE on root.sg2");
    nSession.setSchemaTemplate("t1", "root.sg1.d1");
    nSession.setSchemaTemplate("t1", "root.sg2.d1");
    nSession.createTimeseriesOfTemplateOnPath("root.sg1.d1.v1");
    nSession.createTimeseriesOfTemplateOnPath("root.sg2.d1.v1");

    try {
      nSession.deactivateTemplateOn("t1", "root.sg1.d1.*");
    } catch (Exception e) {
      assertEquals(
          "602: No permissions for this operation, please add privilege "
              + PrivilegeType.values()[
                  AuthorityChecker.translateToPermissionId(
                      Operator.OperatorType.DEACTIVATE_TEMPLATE)],
          e.getMessage());
    }

    session.close();
    nSession.close();
    EnvironmentUtils.restartDaemon();

    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    nSession = new Session("127.0.0.1", 6667, "tpl_user", "tpl_pwd", ZoneId.of("+05:00"));
    session.open();
    nSession.open();
    assertEquals(2, nSession.showPathsTemplateUsingOn("t1").size());

    session.executeNonQueryStatement(
        "grant user tpl_user privileges DELETE_TIMESERIES on root.sg2");
    nSession.deactivateTemplateOn("t1", "root.sg2.**");
    assertEquals(1, nSession.showPathsTemplateUsingOn("t1").size());

    try {
      nSession.deactivateTemplateOn("t1", "root.sg1.d1.*");
    } catch (Exception e) {
      assertEquals(
          "602: No permissions for this operation, please add privilege "
              + PrivilegeType.values()[
                  AuthorityChecker.translateToPermissionId(
                      Operator.OperatorType.DEACTIVATE_TEMPLATE)],
          e.getMessage());
    }

    session.executeNonQueryStatement(
        "grant user tpl_user privileges DELETE_TIMESERIES on root.sg1");
    nSession.deactivateTemplateOn("t1", "root.sg1.**");
    assertEquals(0, nSession.showPathsTemplateUsingOn("t1").size());
    nSession.unsetSchemaTemplate("root.sg1.d1", "t1");
    nSession.unsetSchemaTemplate("root.sg2.d1", "t1");
    nSession.dropSchemaTemplate("t1");
    assertEquals(0, nSession.showAllTemplates().size());

    session.close();
    nSession.close();
    EnvironmentUtils.restartDaemon();

    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    nSession = new Session("127.0.0.1", 6667, "tpl_user", "tpl_pwd", ZoneId.of("+05:00"));
    session.open();
    nSession.open();
    assertEquals(0, nSession.showAllTemplates().size());

    nSession.close();
  }

  @Test
  public void testUnsetTemplateSQL()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    session.createSchemaTemplate(getTemplate("template1"));
    session.setSchemaTemplate("template1", "root.sg.v1");
    session.createTimeseriesOfTemplateOnPath("root.sg.v1.dd1");
    session.executeNonQueryStatement("deactivate schema template template1 from root.sg.v1.dd1");
    assertEquals(0, session.showPathsTemplateUsingOn("template1").size());

    session.createTimeseriesOfTemplateOnPath("root.sg.v1.dd2");
    session.createTimeseriesOfTemplateOnPath("root.sg.v1.dd3");
    session.createTimeseriesOfTemplateOnPath("root.sg.v1.g1.d1");
    assertEquals(3, session.showPathsTemplateUsingOn("template1").size());

    session.executeNonQueryStatement("deactivate schema template template1 from root.sg.v1.*");
    assertEquals(1, session.showPathsTemplateUsingOn("template1").size());

    session.executeNonQueryStatement("deactivate schema template template1 from root.sg.v1.**");
    assertEquals(0, session.showPathsTemplateUsingOn("template1").size());
    session.unsetSchemaTemplate("root.sg.v1", "template1");
    session.dropSchemaTemplate("template1");
    assertEquals(0, session.showAllTemplates().size());
  }

  @Test
  public void testActivateTemplate()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    Template temp1 = getTemplate("template1");

    assertEquals("[]", session.showAllTemplates().toString());

    session.createSchemaTemplate(temp1);

    session.setSchemaTemplate("template1", "root.sg.v1");

    try {
      session.createTimeseriesOfTemplateOnPath("root.sg.v2");
      fail();
    } catch (Exception e) {
      assertEquals("303: Path [root.sg.v2] has not been set any template.", e.getMessage());
    }

    assertEquals(
        new HashSet<>(Collections.singletonList("Time")),
        new HashSet<>(session.executeQueryStatement("SELECT * FROM root.**").getColumnNames()));

    session.createTimeseriesOfTemplateOnPath("root.sg.v1.d1");
    assertEquals("[root.sg.v1.d1]", session.showPathsTemplateUsingOn("template1").toString());

    assertEquals(
        new HashSet<>(
            Arrays.asList(
                "Time",
                "root.sg.v1.d1.x",
                "root.sg.v1.d1.y",
                "root.sg.v1.d1.GPS.x",
                "root.sg.v1.d1.GPS.y",
                "root.sg.v1.d1.vehicle.x",
                "root.sg.v1.d1.vehicle.y",
                "root.sg.v1.d1.vehicle.GPS.x",
                "root.sg.v1.d1.vehicle.GPS.y")),
        new HashSet<>(session.executeQueryStatement("SELECT * FROM root.**").getColumnNames()));

    try {
      session.unsetSchemaTemplate("root.sg.v1", "template1");
      fail();
    } catch (Exception e) {
      assertEquals("326: Template is in use on root.sg.v1.d1", e.getMessage());
    }
  }

  @Test
  public void testSingleMeasurementTemplateAlignment() throws IoTDBConnectionException {
    try {
      List<String> str_list = new ArrayList<>();
      str_list.add("at1");
      List<TSDataType> type_list = new ArrayList<>();
      type_list.add(TSDataType.FLOAT);
      List<TSEncoding> encoding_list = new ArrayList<>();
      encoding_list.add(TSEncoding.GORILLA);
      List<CompressionType> compression_type_list = new ArrayList<>();
      compression_type_list.add(CompressionType.SNAPPY);
      session.createSchemaTemplate(
          "t1", str_list, type_list, encoding_list, compression_type_list, true);
      session.addAlignedMeasurementInTemplate(
          "t1", "at2", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);
      session.addAlignedMeasurementInTemplate(
          "t1", "at3", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);

      session.executeNonQueryStatement("set template t1 to root.SG1");
      session.executeNonQueryStatement("create timeseries of schema template on root.SG1.a");
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      if (session != null) {
        session.close();
      }
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try {
      session = new Session("127.0.0.1", 6667, "root", "root", 16);
      session.open();

      SessionDataSet res = session.executeQueryStatement("show devices");
      if (res.hasNext()) {
        Assert.assertEquals("true", res.next().getFields().get(1).toString());
      }

      res = session.executeQueryStatement("show timeseries");
      int cnt = 0;
      while (res.hasNext()) {
        cnt++;
        res.next();
      }
      assertEquals(3, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private Template getTemplate(String name) throws StatementExecutionException {
    Template sessionTemplate = new Template(name, true);
    TemplateNode iNodeGPS = new InternalNode("GPS", false);
    TemplateNode iNodeV = new InternalNode("vehicle", true);
    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    TemplateNode mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    iNodeGPS.addChild(mNodeX);
    iNodeGPS.addChild(mNodeY);
    iNodeV.addChild(mNodeX);
    iNodeV.addChild(mNodeY);
    iNodeV.addChild(iNodeGPS);
    sessionTemplate.addToTemplate(iNodeGPS);
    sessionTemplate.addToTemplate(iNodeV);
    sessionTemplate.addToTemplate(mNodeX);
    sessionTemplate.addToTemplate(mNodeY);

    return sessionTemplate;
  }
}
