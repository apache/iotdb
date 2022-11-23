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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeMeasurementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterSchemaTreeTest {

  @Test
  public void testSchemaTreeVisitor() throws Exception {

    SchemaNode root = generateSchemaTree();

    testSchemaTree(root);
  }

  @Test
  public void testMultiWildcard() throws IllegalPathException {
    SchemaNode root = generateSchemaTreeWithInternalRepeatedName();

    SchemaTreeMeasurementVisitor visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.**.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        4,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s", "root.a.a.a.s", "root.a.a.s"},
        null,
        new boolean[] {false, false, false, false});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.*.**.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        4,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s", "root.a.a.a.s", "root.a.a.s"},
        null,
        new boolean[] {false, false, false, false});

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.a.**.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        3,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s", "root.a.a.a.s"},
        null,
        new boolean[] {false, false, false});

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.a.**.*.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s"},
        null,
        new boolean[] {false, false, false});

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.a.**.a.*.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s"},
        null,
        new boolean[] {false, false, false});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.c.s1"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.c.c.c.d.c.c.s1", "root.c.c.c.d.c.s1"},
        null,
        new boolean[] {false, false});

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.c.d.c.s1"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.c.c.c.d.c.s1"}, null, new boolean[] {false});

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.d.**.c.s1"), 0, 0, false);
    checkVisitorResult(
        visitor, 1, new String[] {"root.c.c.c.d.c.c.s1"}, null, new boolean[] {false});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.d.*.*"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.c.c.c.d.c.s1"}, null, new boolean[] {false});
  }

  private void testSchemaTree(SchemaNode root) throws Exception {

    SchemaTreeMeasurementVisitor visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.d2.a.s1"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.sg.d2.a.s1"}, null, new boolean[] {true});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.*.s2"), 0, 0, false);
    checkVisitorResult(
        visitor, 2, new String[] {"root.sg.d1.s2", "root.sg.d2.s2"}, new String[] {"", ""}, null);

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.*.status"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        null);

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.d2.*.*"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.d1"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.*.a"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true},
        new int[] {0, 0});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.*.*"), 2, 2, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", ""},
        new boolean[] {false, false},
        new int[] {3, 4});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.*"), 2, 3, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1"},
        new String[] {"", ""},
        new boolean[] {true, false},
        new int[] {4, 5});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.d1.**"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.d2.**"), 3, 1, true);
    checkVisitorResult(
        visitor,
        3,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", "", ""},
        new boolean[] {true, false, false},
        new int[] {2, 3, 4});

    visitor =
        new SchemaTreeMeasurementVisitor(root, new PartialPath("root.sg.**.status"), 2, 1, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        new boolean[] {true, false},
        new int[] {2, 3});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.*"), 10, 0, false);
    checkVisitorResult(
        visitor,
        6,
        new String[] {
          "root.sg.d1.s1",
          "root.sg.d1.s2",
          "root.sg.d2.a.s1",
          "root.sg.d2.a.s2",
          "root.sg.d2.s1",
          "root.sg.d2.s2"
        },
        new String[] {"", "", "", "", "", ""},
        new boolean[] {false, false, true, true, false, false},
        new int[] {1, 2, 3, 4, 5, 6});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.**.*.**"), 10, 0, false);
    checkVisitorResult(
        visitor,
        6,
        new String[] {
          "root.sg.d1.s1",
          "root.sg.d1.s2",
          "root.sg.d2.a.s1",
          "root.sg.d2.a.s2",
          "root.sg.d2.s1",
          "root.sg.d2.s2"
        },
        new String[] {"", "", "", "", "", ""},
        new boolean[] {false, false, true, true, false, false},
        new int[] {1, 2, 3, 4, 5, 6});

    visitor = new SchemaTreeMeasurementVisitor(root, new PartialPath("root.*.**.**"), 10, 0, false);
    checkVisitorResult(
        visitor,
        6,
        new String[] {
          "root.sg.d1.s1",
          "root.sg.d1.s2",
          "root.sg.d2.a.s1",
          "root.sg.d2.a.s2",
          "root.sg.d2.s1",
          "root.sg.d2.s2"
        },
        new String[] {"", "", "", "", "", ""},
        new boolean[] {false, false, true, true, false, false},
        new int[] {1, 2, 3, 4, 5, 6});
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

  /**
   * Generate the following tree: root.a.s, root.a.a.s, root.a.a.a.s, root.a.a.a.a.s,
   * root.a.a.a.a.a.s, root.c.c.c.d.c.s1, root.c.c.c.d.c.c.s1
   *
   * @return the root node of the generated schemTree
   */
  private SchemaNode generateSchemaTreeWithInternalRepeatedName() {
    SchemaNode root = new SchemaInternalNode("root");

    SchemaNode parent = root;
    SchemaNode a;
    MeasurementSchema schema = new MeasurementSchema("s", TSDataType.INT32);
    SchemaNode s;
    for (int i = 0; i < 5; i++) {
      a = new SchemaEntityNode("a");
      s = new SchemaMeasurementNode("s", schema);
      a.addChild("s", s);
      parent.addChild("a", a);
      parent = a;
    }

    parent = root;
    SchemaNode c;
    for (int i = 0; i < 3; i++) {
      c = new SchemaInternalNode("c");
      parent.addChild("c", c);
      parent = c;
    }

    SchemaNode d = new SchemaInternalNode("d");
    parent.addChild("d", d);
    parent = d;

    for (int i = 0; i < 2; i++) {
      c = new SchemaEntityNode("c");
      c.addChild("s1", new SchemaMeasurementNode("s1", schema));
      parent.addChild("c", c);
      parent = c;
    }

    return root;
  }

  private void checkVisitorResult(
      SchemaTreeMeasurementVisitor visitor,
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
      SchemaTreeMeasurementVisitor visitor,
      int expectedNum,
      String[] expectedPath,
      String[] expectedAlias,
      boolean[] expectedAligned,
      int[] expectedOffset) {
    checkVisitorResult(visitor, expectedNum, expectedPath, expectedAlias, expectedAligned);

    visitor.reset();
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
    ISchemaTree schemaTree = new ClusterSchemaTree(generateSchemaTree());

    testSearchDeviceInfo(schemaTree);
  }

  private void testSearchDeviceInfo(ISchemaTree schemaTree) throws Exception {
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
  public void testGetMatchedDevices() throws Exception {
    ISchemaTree schemaTree = new ClusterSchemaTree(generateSchemaTree());

    List<DeviceSchemaInfo> deviceSchemaInfoList =
        schemaTree.getMatchedDevices(new PartialPath("root.sg.d2.a"), false);
    Assert.assertEquals(1, deviceSchemaInfoList.size());
    DeviceSchemaInfo deviceSchemaInfo = deviceSchemaInfoList.get(0);
    Assert.assertEquals(new PartialPath("root.sg.d2.a"), deviceSchemaInfo.getDevicePath());
    Assert.assertTrue(deviceSchemaInfo.isAligned());
    Assert.assertEquals(2, deviceSchemaInfo.getMeasurements(Sets.newSet("*")).size());

    deviceSchemaInfoList = schemaTree.getMatchedDevices(new PartialPath("root.sg.*"), false);
    deviceSchemaInfoList.sort(Comparator.comparing(DeviceSchemaInfo::getDevicePath));
    Assert.assertEquals(2, deviceSchemaInfoList.size());
    Assert.assertEquals(new PartialPath("root.sg.d1"), deviceSchemaInfoList.get(0).getDevicePath());
    Assert.assertEquals(new PartialPath("root.sg.d2"), deviceSchemaInfoList.get(1).getDevicePath());

    deviceSchemaInfoList = schemaTree.getMatchedDevices(new PartialPath("root.sg.**"), false);
    deviceSchemaInfoList.sort(Comparator.comparing(DeviceSchemaInfo::getDevicePath));
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    Assert.assertEquals(new PartialPath("root.sg.d1"), deviceSchemaInfoList.get(0).getDevicePath());
    Assert.assertEquals(new PartialPath("root.sg.d2"), deviceSchemaInfoList.get(1).getDevicePath());
    Assert.assertEquals(
        new PartialPath("root.sg.d2.a"), deviceSchemaInfoList.get(2).getDevicePath());

    deviceSchemaInfoList = schemaTree.getMatchedDevices(new PartialPath("root.**"), false);
    deviceSchemaInfoList.sort(Comparator.comparing(DeviceSchemaInfo::getDevicePath));
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    Assert.assertEquals(new PartialPath("root.sg.d1"), deviceSchemaInfoList.get(0).getDevicePath());
    Assert.assertEquals(new PartialPath("root.sg.d2"), deviceSchemaInfoList.get(1).getDevicePath());
    Assert.assertEquals(
        new PartialPath("root.sg.d2.a"), deviceSchemaInfoList.get(2).getDevicePath());
  }

  @Test
  public void testSerialization() throws Exception {
    SchemaNode root = generateSchemaTree();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    root.serialize(outputStream);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    ISchemaTree schemaTree = ClusterSchemaTree.deserialize(inputStream);

    Pair<List<MeasurementPath>, Integer> visitResult =
        schemaTree.searchMeasurementPaths(new PartialPath("root.sg.**.status"), 2, 1, true);
    Assert.assertEquals(2, visitResult.left.size());
    Assert.assertEquals(3, (int) visitResult.right);

    testSearchDeviceInfo(schemaTree);
  }

  @Test
  public void testAppendMeasurementPath() throws Exception {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
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
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    for (ClusterSchemaTree tree : generateSchemaTrees()) {
      schemaTree.mergeSchemaTree(tree);
    }

    testSchemaTree(schemaTree.getRoot());
  }

  @Test
  public void testMergeSchemaTreeAndSearchDeviceSchemaInfo() throws Exception {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    for (ClusterSchemaTree tree : generateSchemaTrees()) {
      schemaTree.mergeSchemaTree(tree);
    }
    PartialPath devicePath = new PartialPath("root.sg.d99999");
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    schemaTree.searchDeviceSchemaInfo(devicePath, measurements);
  }

  private List<ClusterSchemaTree> generateSchemaTrees() throws Exception {
    List<ClusterSchemaTree> schemaTreeList = new ArrayList<>();
    ClusterSchemaTree schemaTree;
    List<MeasurementPath> measurementPathList = generateMeasurementPathList();
    List<MeasurementPath> list;
    for (int i = 0; i < 6; i += 2) {
      list = new ArrayList<>();
      list.add(measurementPathList.get(i));
      list.add(measurementPathList.get(i + 1));

      schemaTree = new ClusterSchemaTree();
      schemaTree.appendMeasurementPaths(list);
      schemaTreeList.add(schemaTree);
    }
    return schemaTreeList;
  }

  @Test
  public void testNestedDevice() throws Exception {
    List<MeasurementPath> measurementPathList = new ArrayList<>();

    MeasurementSchema schema1 = new MeasurementSchema("s1", TSDataType.INT32);

    MeasurementPath measurementPath = new MeasurementPath("root.sg.d1.a.s1");
    measurementPath.setMeasurementSchema(schema1);
    measurementPathList.add(measurementPath);

    measurementPath = new MeasurementPath("root.sg.d1.s1");
    measurementPath.setMeasurementSchema(schema1);
    measurementPath.setUnderAlignedEntity(true);
    measurementPathList.add(measurementPath);

    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    schemaTree.appendMeasurementPaths(measurementPathList);

    Assert.assertTrue(
        schemaTree
            .searchDeviceSchemaInfo(new PartialPath("root.sg.d1"), Collections.singletonList("s1"))
            .isAligned());
  }
}
