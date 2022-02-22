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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TemplateTest {
  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testTemplate() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    MManager manager = (MManager) IoTDB.metaManager;
    manager.createSchemaTemplate(plan);

    // set device template
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg1.d1");

    manager.setSchemaTemplate(setTemplatePlan);

    IMNode node = manager.getDeviceNode(new PartialPath("root.sg1.d1"));
    node = manager.setUsingSchemaTemplate(node);

    MeasurementSchema s11 =
        new MeasurementSchema("s11", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    assertNotNull(node.getSchemaTemplate());

    Set<IMeasurementSchema> allSchema =
        new HashSet<>(node.getSchemaTemplate().getSchemaMap().values());
    manager.getAllMeasurementByDevicePath(new PartialPath("root.sg1.d1")).stream()
        .map(MeasurementPath::getMeasurementSchema)
        .forEach(allSchema::remove);

    assertTrue(allSchema.isEmpty());

    IMeasurementMNode mNode = manager.getMeasurementMNode(new PartialPath("root.sg1.d1.s11"));
    IMeasurementMNode mNode2 =
        manager.getMeasurementMNode(new PartialPath("root.sg1.d1.vector.s2"));
    assertNotNull(mNode);
    assertEquals(mNode.getSchema(), s11);
    assertNotNull(mNode2);
    assertEquals(
        mNode2.getSchema(), manager.getTemplate("template1").getSchemaMap().get("vector.s2"));

    try {
      manager.getMeasurementMNode(new PartialPath("root.sg1.d1.s100"));
      fail();
    } catch (PathNotExistException e) {
      assertEquals("Path [root.sg1.d1.s100] does not exist", e.getMessage());
    }
  }

  @Test
  public void testTemplateInnerTree() {
    CreateTemplatePlan plan = getTreeTemplatePlan();
    Template template;
    MManager manager = (MManager) IoTDB.metaManager;

    try {
      manager.createSchemaTemplate(plan);
      template = manager.getTemplate("treeTemplate");
      assertEquals(4, template.getMeasurementsCount());
      assertEquals("d1", template.getPathNodeInTemplate("d1").getName());
      assertEquals(null, template.getPathNodeInTemplate("notExists"));
      assertEquals("[GPS]", template.getAllAlignedPrefix().toString());

      String[] alignedMeasurements = {"to.be.prefix.s1", "to.be.prefix.s2"};
      TSDataType[] dataTypes = {TSDataType.INT32, TSDataType.INT32};
      TSEncoding[] encodings = {TSEncoding.RLE, TSEncoding.RLE};
      CompressionType[] compressionTypes = {CompressionType.SNAPPY, CompressionType.SNAPPY};
      template.addAlignedMeasurements(alignedMeasurements, dataTypes, encodings, compressionTypes);

      assertEquals("[to.be.prefix, GPS]", template.getAllAlignedPrefix().toString());
      assertEquals("[s1, s2]", template.getAlignedMeasurements("to.be.prefix").toString());

      template.deleteAlignedPrefix("to.be.prefix");

      assertEquals("[GPS]", template.getAllAlignedPrefix().toString());
      assertEquals(null, template.getDirectNode("prefix"));
      assertEquals("to", template.getDirectNode("to").getName());

      try {
        template.deleteMeasurements("a.single");
        fail();
      } catch (MetadataException e) {
        assertEquals("Path [a.single] does not exist", e.getMessage());
      }
      assertEquals(
          "[d1.s1, GPS.x, to.be.prefix.s2, GPS.y, to.be.prefix.s1, s2]",
          template.getAllMeasurementsPaths().toString());

      template.deleteSeriesCascade("to");

      assertEquals("[d1.s1, GPS.x, GPS.y, s2]", template.getAllMeasurementsPaths().toString());

    } catch (MetadataException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCreateSchemaTemplateSerialization() throws IOException {
    CreateTemplatePlan plan = getTreeTemplatePlan();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plan.serialize(dos);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);

    assertEquals(PhysicalPlan.PhysicalPlanType.CREATE_TEMPLATE.ordinal(), buffer.get());

    CreateTemplatePlan deserializedPlan = new CreateTemplatePlan();
    deserializedPlan.deserialize(buffer);

    assertEquals(
        plan.getCompressors().get(0).get(0), deserializedPlan.getCompressors().get(0).get(0));
    assertEquals(plan.getMeasurements().size(), deserializedPlan.getMeasurements().size());
    assertEquals(plan.getName(), deserializedPlan.getName());
  }

  private CreateTemplatePlan getTreeTemplatePlan() {
    /**
     * Construct a template like: create schema template treeTemplate ( (d1.s1 INT32 GORILLA
     * SNAPPY), (s2 INT32 GORILLA SNAPPY), (GPS.x FLOAT RLE SNAPPY), (GPS.y FLOAT RLE SNAPPY), )with
     * aligned (GPS)
     */
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("d1.s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Arrays.asList("GPS.x", "GPS.y"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Arrays.asList(TSEncoding.RLE, TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.UNCOMPRESSED));
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));

    return new CreateTemplatePlan(
        "treeTemplate", measurementList, dataTypeList, encodingList, compressionTypes);
  }

  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("vector.s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> compressorList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      compressorList.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(compressorList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s21");
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  private CreateTemplatePlan getDirectAlignedTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("vs" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> compressorList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      compressorList.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(compressorList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "templateDA", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  /**
   * Test for show templates, including all templates, paths set or using designated template
   *
   * @throws MetadataException
   */
  @Test
  public void testShowTemplates() throws MetadataException, QueryProcessException {
    MManager manager = (MManager) IoTDB.metaManager;
    assertEquals(0, manager.getAllTemplates().size());
    CreateTemplatePlan plan1 = getTreeTemplatePlan();
    CreateTemplatePlan plan2 = getCreateTemplatePlan();
    manager.createSchemaTemplate(plan1);
    manager.createSchemaTemplate(plan2);

    assertEquals("[template1, treeTemplate]", manager.getAllTemplates().toString());

    for (int i = 0; i < 3; i++) {
      SetTemplatePlan setTemplatePlan =
          new SetTemplatePlan("template1", String.format("root.sg%d.d%d", i, i + 1));
      manager.setSchemaTemplate(setTemplatePlan);
    }

    assertEquals(
        new HashSet<>(Arrays.asList("root.sg1.d2", "root.sg0.d1", "root.sg2.d3")),
        manager.getPathsSetTemplate("*"));
    assertEquals(new HashSet<>(Arrays.asList()), manager.getPathsSetTemplate("treeTemplate"));

    for (int i = 0; i < 3; i++) {
      SetTemplatePlan setTemplatePlan =
          new SetTemplatePlan("treeTemplate", String.format("root.tsg%d.d%d", i + 9, i + 10));
      manager.setSchemaTemplate(setTemplatePlan);
    }

    assertEquals(
        new HashSet<>(Arrays.asList("root.tsg10.d11", "root.tsg11.d12", "root.tsg9.d10")),
        manager.getPathsSetTemplate("treeTemplate"));
    assertEquals(
        new HashSet<>(
            Arrays.asList(
                "root.tsg10.d11",
                "root.tsg11.d12",
                "root.tsg9.d10",
                "root.sg1.d2",
                "root.sg0.d1",
                "root.sg2.d3")),
        manager.getPathsSetTemplate("*"));

    PlanExecutor exe1 = new PlanExecutor();
    exe1.insert(getInsertRowPlan("root.sg0.d1", "s11"));
    exe1.insert(getInsertRowPlan("root.sg1.d2", "s11"));
    exe1.insert(getInsertRowPlan("root.tsg10.d11.d1", "s1"));

    assertEquals(
        new HashSet<>(Arrays.asList("root.tsg10.d11", "root.sg1.d2", "root.sg0.d1")),
        manager.getPathsUsingTemplate("*"));

    try {
      manager.createSchemaTemplate(plan1);
      fail();
    } catch (MetadataException e) {
      assertEquals("Duplicated template name: treeTemplate", e.getMessage());
    }

    try {
      manager.dropSchemaTemplate(new DropTemplatePlan("treeTemplate"));
      fail();
    } catch (MetadataException e) {
      assertEquals(
          "Template [treeTemplate] has been set on MTree, cannot be dropped now.", e.getMessage());
    }
  }

  @Test
  public void testShowAllSchemas() throws MetadataException {
    MManager manager = (MManager) IoTDB.metaManager;
    CreateTemplatePlan plan1 = getTreeTemplatePlan();
    CreateTemplatePlan plan2 = getCreateTemplatePlan();
    manager.createSchemaTemplate(plan1);
    manager.createSchemaTemplate(plan2);
    assertEquals(4, manager.getSchemasInTemplate("treeTemplate", "").size());
    assertEquals(2, manager.getSchemasInTemplate("treeTemplate", "GPS").size());
    assertEquals(11, manager.getSchemasInTemplate("template1", "").size());
    assertEquals(10, manager.getSchemasInTemplate("template1", "vector").size());
  }

  @Test
  public void testDropTemplate() throws MetadataException {
    MManager manager = (MManager) IoTDB.metaManager;
    CreateTemplatePlan plan1 = getTreeTemplatePlan();
    CreateTemplatePlan plan2 = getCreateTemplatePlan();
    manager.createSchemaTemplate(plan1);
    manager.createSchemaTemplate(plan2);

    assertEquals("[template1, treeTemplate]", manager.getAllTemplates().toString());

    try {
      manager.createSchemaTemplate(plan2);
      fail();
    } catch (MetadataException e) {
      assertEquals("Duplicated template name: template1", e.getMessage());
    }

    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg.d0");
    manager.setSchemaTemplate(setTemplatePlan);

    try {
      manager.dropSchemaTemplate(new DropTemplatePlan("template1"));
      fail();
    } catch (MetadataException e) {
      assertEquals(
          "Template [template1] has been set on MTree, cannot be dropped now.", e.getMessage());
    }

    UnsetTemplatePlan unsetPlan = new UnsetTemplatePlan("root.sg.d0", "template1");
    manager.unsetSchemaTemplate(unsetPlan);

    manager.dropSchemaTemplate(new DropTemplatePlan("template1"));
    assertEquals("[treeTemplate]", manager.getAllTemplates().toString());
    manager.createSchemaTemplate(plan2);
    assertEquals("[template1, treeTemplate]", manager.getAllTemplates().toString());
    manager.dropSchemaTemplate(new DropTemplatePlan("template1"));
    manager.dropSchemaTemplate(new DropTemplatePlan("treeTemplate"));
  }

  @Test
  public void testTemplateAlignment() throws MetadataException {
    MManager manager = (MManager) IoTDB.metaManager;
    manager.createTimeseries(
        new PartialPath("root.laptop.d0"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null);
    manager.createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null);
    manager.createTimeseries(
        new PartialPath("root.laptop.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null);
    manager.createTimeseries(
        new PartialPath("root.laptop.d1.ss.t1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null);

    manager.createSchemaTemplate(getDirectAlignedTemplate());
    try {
      manager.setSchemaTemplate(new SetTemplatePlan("templateDA", "root.laptop.d1"));
      fail();
    } catch (Exception e) {
      assertEquals(
          "Template[templateDA] and mounted node[root.laptop.d1.vs0] has different alignment.",
          e.getMessage());
    }
  }

  private InsertRowPlan getInsertRowPlan(String prefixPath, String measurement)
      throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT64};

    String[] columns = new String[1];
    columns[0] = "1";

    return new InsertRowPlan(
        new PartialPath(prefixPath), time, new String[] {measurement}, dataTypes, columns);
  }
}
