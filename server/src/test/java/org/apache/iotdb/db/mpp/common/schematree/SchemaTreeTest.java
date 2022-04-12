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
package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaTreeTest {

  @Test
  public void testSchemaTreeVisitor() throws Exception {

    SchemaNode root = generateSchemaTree();

    testSchemaTree(root);
  }

  private void testSchemaTree(SchemaNode root) throws Exception {

    SchemaTreeVisitor visitor =
        new SchemaTreeVisitor(root, new PartialPath("root.sg.d2.a.s1"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.sg.d2.a.s1"}, null, new boolean[] {true});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.s2"), 0, 0, false);
    checkVisitorResult(
        visitor, 2, new String[] {"root.sg.d1.s2", "root.sg.d2.s2"}, new String[] {"", ""}, null);

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.status"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        null);

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d2.*.*"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d1"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.a"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true},
        new int[] {0, 0});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.*"), 2, 2, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", ""},
        new boolean[] {false, false},
        new int[] {3, 4});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*"), 2, 3, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1"},
        new String[] {"", ""},
        new boolean[] {true, false},
        new int[] {4, 5});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d1.**"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d2.**"), 3, 1, true);
    checkVisitorResult(
        visitor,
        3,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", "", ""},
        new boolean[] {true, false, false},
        new int[] {2, 3, 4});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.**.status"), 2, 1, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.s2", "root.sg.d2.a.s2"},
        new String[] {"status", "status"},
        new boolean[] {false, true},
        new int[] {2, 3});
  }

  /**
   * Generate the following tree: root.sg.d1.s1, root.sg.d1.s2(status) root.sg.d2.s1,
   * root.sg.d2.s2(status) root.sg.d2.a.s1, root.sg.d2.a.s2(status)
   *
   * @return the root node of the generated schemTree
   */
  private SchemaNode generateSchemaTree() {
    SchemaNode root = new SchemaInternalNode("root");

    SchemaNode sg = new SchemaInternalNode("sg");
    root.addChild("sg", sg);

    SchemaEntityNode d1 = new SchemaEntityNode("d1");
    sg.addChild("d1", d1);

    MeasurementSchema schema1 = new MeasurementSchema("s1", TSDataType.INT32);
    MeasurementSchema schema2 = new MeasurementSchema("s2", TSDataType.INT64);
    SchemaMeasurementNode s1 = new SchemaMeasurementNode("s1", schema1);
    d1.addChild("s1", s1);
    SchemaMeasurementNode s2 = new SchemaMeasurementNode("s2", schema2);
    s2.setAlias("status");
    d1.addChild("s2", s2);
    d1.addAliasChild("status", s2);

    SchemaEntityNode d2 = new SchemaEntityNode("d2");
    sg.addChild("d2", d2);
    d2.addChild("s1", s1);
    d2.addChild("s2", s2);
    d2.addAliasChild("status", s2);

    SchemaEntityNode a = new SchemaEntityNode("a");
    a.setAligned(true);
    d2.addChild("a", a);
    a.addChild("s1", s1);
    a.addChild("s2", s2);
    a.addAliasChild("status", s2);

    return root;
  }

  private void checkVisitorResult(
      SchemaTreeVisitor visitor,
      int expectedNum,
      String[] expectedPath,
      String[] expectedAlias,
      boolean[] expectedAligned) {
    List<MeasurementPath> result = visitor.getAllResult();
    Assert.assertEquals(expectedNum, result.size());
    for (int i = 0; i < expectedNum; i++) {
      Assert.assertEquals(expectedPath[i], result.get(i).getFullPath());
    }

    if (expectedAlias != null) {
      for (int i = 0; i < expectedNum; i++) {
        Assert.assertEquals(expectedAlias[i], result.get(i).getMeasurementAlias());
      }
    }

    if (expectedAligned != null) {
      for (int i = 0; i < expectedNum; i++) {
        Assert.assertEquals(expectedAligned[i], result.get(i).isUnderAlignedEntity());
      }
    }
  }

  private void checkVisitorResult(
      SchemaTreeVisitor visitor,
      int expectedNum,
      String[] expectedPath,
      String[] expectedAlias,
      boolean[] expectedAligned,
      int[] expectedOffset) {
    checkVisitorResult(visitor, expectedNum, expectedPath, expectedAlias, expectedAligned);

    visitor.resetStatus();
    int i = 0;
    MeasurementPath result;
    while (visitor.hasNext()) {
      result = visitor.next();
      Assert.assertEquals(expectedPath[i], result.getFullPath());
      Assert.assertEquals(expectedAlias[i], result.getMeasurementAlias());
      Assert.assertEquals(expectedAligned[i], result.isUnderAlignedEntity());
      Assert.assertEquals(expectedOffset[i], visitor.getNextOffset());
      i++;
    }
    Assert.assertEquals(expectedNum, i);
  }

  @Test
  public void testSearchDeviceInfo() throws Exception {
    SchemaTree schemaTree = new SchemaTree(generateSchemaTree());

    testSearchDeviceInfo(schemaTree);
  }

  private void testSearchDeviceInfo(SchemaTree schemaTree) throws Exception {
    PartialPath devicePath = new PartialPath("root.sg.d1");
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");

    DeviceSchemaInfo deviceSchemaInfo = schemaTree.searchDeviceSchemaInfo(devicePath, measurements);
    Assert.assertEquals(
        measurements,
        deviceSchemaInfo.getMeasurementSchemaList().stream()
            .map(MeasurementSchema::getMeasurementId)
            .collect(Collectors.toList()));

    devicePath = new PartialPath("root.sg.d2.a");
    measurements.remove(1);
    measurements.add("status");
    deviceSchemaInfo = schemaTree.searchDeviceSchemaInfo(devicePath, measurements);
    Assert.assertTrue(deviceSchemaInfo.isAligned());
    measurements.remove(1);
    measurements.add("s2");
    Assert.assertEquals(
        measurements,
        deviceSchemaInfo.getMeasurementSchemaList().stream()
            .map(MeasurementSchema::getMeasurementId)
            .collect(Collectors.toList()));
  }

  @Test
  public void testSerialization() throws Exception {
    SchemaNode root = generateSchemaTree();
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    root.serialize(buffer);
    buffer.flip();

    SchemaTree schemaTree = SchemaTree.deserialize(buffer);

    Pair<List<MeasurementPath>, Integer> visitResult =
        schemaTree.searchMeasurementPaths(new PartialPath("root.sg.**.status"), 2, 1, true);
    Assert.assertEquals(2, visitResult.left.size());
    Assert.assertEquals(3, (int) visitResult.right);

    testSearchDeviceInfo(schemaTree);
  }

  @Test
  public void testAppendMeasurementPath() throws Exception {
    SchemaTree schemaTree = new SchemaTree();
    List<MeasurementPath> measurementPathList = generateMeasurementPathList();

    schemaTree.appendMeasurementPaths(measurementPathList);

    testSchemaTree(schemaTree.getRoot());
  }

  /**
   * Generate the following tree: root.sg.d1.s1, root.sg.d1.s2(status) root.sg.d2.s1,
   * root.sg.d2.s2(status) root.sg.d2.a.s1, root.sg.d2.a.s2(status)
   */
  private List<MeasurementPath> generateMeasurementPathList() throws Exception {
    List<MeasurementPath> measurementPathList = new ArrayList<>();

    MeasurementSchema schema1 = new MeasurementSchema("s1", TSDataType.INT32);
    MeasurementSchema schema2 = new MeasurementSchema("s2", TSDataType.INT64);

    MeasurementPath measurementPath = new MeasurementPath("root.sg.d1.s1");
    measurementPath.setMeasurementSchema(schema1);
    measurementPathList.add(measurementPath);

    measurementPath = new MeasurementPath("root.sg.d1.s2");
    measurementPath.setMeasurementSchema(schema2);
    measurementPath.setMeasurementAlias("status");
    measurementPathList.add(measurementPath);

    measurementPath = new MeasurementPath("root.sg.d2.a.s1");
    measurementPath.setMeasurementSchema(schema1);
    measurementPath.setUnderAlignedEntity(true);
    measurementPathList.add(measurementPath);

    measurementPath = new MeasurementPath("root.sg.d2.a.s2");
    measurementPath.setMeasurementSchema(schema2);
    measurementPath.setMeasurementAlias("status");
    measurementPath.setUnderAlignedEntity(true);
    measurementPathList.add(measurementPath);

    measurementPath = new MeasurementPath("root.sg.d2.s1");
    measurementPath.setMeasurementSchema(schema1);
    measurementPathList.add(measurementPath);

    measurementPath = new MeasurementPath("root.sg.d2.s2");
    measurementPath.setMeasurementSchema(schema2);
    measurementPath.setMeasurementAlias("status");
    measurementPathList.add(measurementPath);

    return measurementPathList;
  }

  @Test
  public void testMergeSchemaTree() throws Exception {
    SchemaTree schemaTree = new SchemaTree();
    for (SchemaTree tree : generateSchemaTrees()) {
      schemaTree.mergeSchemaTree(tree);
    }

    testSchemaTree(schemaTree.getRoot());
  }

  private List<SchemaTree> generateSchemaTrees() throws Exception {
    List<SchemaTree> schemaTreeList = new ArrayList<>();
    SchemaTree schemaTree;
    List<MeasurementPath> measurementPathList = generateMeasurementPathList();
    List<MeasurementPath> list;
    for (int i = 0; i < 6; i += 2) {
      list = new ArrayList<>();
      list.add(measurementPathList.get(i));
      list.add(measurementPathList.get(i + 1));

      schemaTree = new SchemaTree();
      schemaTree.appendMeasurementPaths(list);
      schemaTreeList.add(schemaTree);
    }
    return schemaTreeList;
  }
}
