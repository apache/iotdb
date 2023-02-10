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
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.template.TemplateNode;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSessionSchemaTemplateIT {

  private ISession session;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    session = EnvFactory.getEnv().getSessionConnection();
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
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
}
