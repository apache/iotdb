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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class TemplateUT {

  private Session session;

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
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
                "root.sg.v6")),
        new HashSet<>(session.showPathsTemplateSetOn("")));

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
        new HashSet<>(session.showPathsTemplateUsingOn("")));
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
