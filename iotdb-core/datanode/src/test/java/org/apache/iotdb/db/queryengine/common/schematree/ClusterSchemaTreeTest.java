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
package org.apache.iotdb.db.queryengine.common.schematree;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.queryengine.common.schematree.visitor.SchemaTreeVisitorFactory;
import org.apache.iotdb.db.queryengine.common.schematree.visitor.SchemaTreeVisitorWithLimitOffsetWrapper;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

public class ClusterSchemaTreeTest {
  private static final Logger logger = LoggerFactory.getLogger(ClusterSchemaTreeTest.class);

  @Test
  @Ignore
  public void testPerformanceOnSimpleTree() throws IllegalPathException {
    long startTime = System.currentTimeMillis();
    int round = 20;
    for (int i = 0; i < round; i++) {
      for (int j = 0; j < 10000; j++) {
        testMultiWildcard();
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("AllTime={}", (endTime - startTime) / round);
  }

  @Test
  @Ignore
  public void testPerformanceOnComplexTree() throws IllegalPathException {
    int deep = 5;
    int width = 5;
    SchemaNode root = generateComplexSchemaTree(deep, width);
    PartialPath path = new PartialPath("root.**.d0.s");
    long startTime = System.currentTimeMillis();
    long calTime = 0;
    int round = 20;
    for (int i = 0; i < round; i++) {
      for (int j = 0; j < 1000; j++) {
        SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor =
            createSchemaTreeVisitorWithLimitOffsetWrapper(root, path, 0, 0, false);

        long calStartTime = System.currentTimeMillis();
        List<MeasurementPath> res = visitor.getAllResult();
        Assert.assertEquals((int) (1 - Math.pow(width, deep)) / (1 - width) - 1, res.size());
        calTime += (System.currentTimeMillis() - calStartTime);
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("CalculateTime={}", calTime / round);
    logger.info("InitialTime={}", (endTime - startTime - calTime) / round);
    logger.info("AllTime={}", (endTime - startTime) / round);
  }

  @Test
  public void testSchemaTreeVisitor() throws Exception {

    SchemaNode root = generateSchemaTree();

    testSchemaTree(root);
  }

  @Test
  public void testMultiWildcard() throws IllegalPathException {
    SchemaNode root = generateSchemaTreeWithInternalRepeatedName();

    SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.**.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        4,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s", "root.a.a.a.s", "root.a.a.s"},
        null,
        new boolean[] {false, false, false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.*.**.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        4,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s", "root.a.a.a.s", "root.a.a.s"},
        null,
        new boolean[] {false, false, false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.a.**.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        3,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s", "root.a.a.a.s"},
        null,
        new boolean[] {false, false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.a.**.*.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s"},
        null,
        new boolean[] {false, false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.a.**.a.*.s"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.a.a.a.a.a.s", "root.a.a.a.a.s"},
        null,
        new boolean[] {false, false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.c.s1"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.c.c.c.d.c.c.s1", "root.c.c.c.d.c.s1"},
        null,
        new boolean[] {false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.c.d.c.s1"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.c.c.c.d.c.s1"}, null, new boolean[] {false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.d.**.c.s1"), 0, 0, false);
    checkVisitorResult(
        visitor, 1, new String[] {"root.c.c.c.d.c.c.s1"}, null, new boolean[] {false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.d.*.*"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.c.c.c.d.c.s1"}, null, new boolean[] {false});
  }

  private void testSchemaTree(SchemaNode root) throws Exception {

    SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.a.s1"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.sg.d2.a.s1"}, null, new boolean[] {true});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.s2"), 0, 0, false);
    checkVisitorResult(
        visitor, 2, new String[] {"root.sg.d1.s2", "root.sg.d2.s2"}, new String[] {"", ""}, null);

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.status"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        null);

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.*.*"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d1"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.a"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true},
        new int[] {0, 0});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.*"), 2, 2, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", ""},
        new boolean[] {false, false},
        new int[] {3, 4});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*"), 2, 3, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1"},
        new String[] {"", ""},
        new boolean[] {true, false},
        new int[] {4, 5});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d1.**"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.**"), 3, 1, true);
    checkVisitorResult(
        visitor,
        3,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", "", ""},
        new boolean[] {true, false, false},
        new int[] {2, 3, 4});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.**.status"), 2, 1, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        new boolean[] {true, false},
        new int[] {2, 3});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.*"), 10, 0, false);
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

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.*.**"), 10, 0, false);
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

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.*.**.**"), 10, 0, false);
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

  /**
   * Generate complex schemaengine tree with specific deep and width. For example, if deep=2 and
   * width=3, the schemaengine tree contains timeseries: root.d0.s, root.d1.s, root.s2.s,
   * root.d0.d0.s, root.d0.d1.s, root.d0.d2.s, root.d1.d0.s, root.d1.d1.s, root.d1.d2.s,
   * root.d2.d0.s, root.d2.d1.s, root.d2.d2.s
   *
   * @param deep deep
   * @param width width
   * @return root node
   */
  private SchemaNode generateComplexSchemaTree(int deep, int width) {
    SchemaNode root = new SchemaInternalNode("root");

    List<SchemaNode> nodes = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema("s", TSDataType.INT32);
    for (int i = 0; i < width; i++) {
      SchemaEntityNode entityNode = new SchemaEntityNode("d" + i);
      nodes.add(entityNode);
      root.addChild("d" + i, entityNode);
    }
    for (int i = 0; i < deep - 1; i++) {
      List<SchemaNode> nextLevelNode = new ArrayList<>();
      for (SchemaNode parent : nodes) {
        SchemaMeasurementNode measurementNode = new SchemaMeasurementNode("s", schema);
        parent.addChild("s", measurementNode);
        for (int j = 0; j < width; j++) {
          SchemaEntityNode entityNode = new SchemaEntityNode("d" + j);
          parent.addChild("d" + j, entityNode);
          nextLevelNode.add(entityNode);
        }
      }
      nodes = nextLevelNode;
    }
    for (SchemaNode parent : nodes) {
      SchemaMeasurementNode measurementNode = new SchemaMeasurementNode("s", schema);
      parent.addChild("s", measurementNode);
    }

    return root;
  }

  @Test
  public void testSchemaTreeWithScope() throws Exception {
    SchemaNode root = generateSchemaTree();
    PathPatternTree scope = new PathPatternTree();
    scope.appendPathPattern(new PartialPath("root.sg.d1.**"));
    scope.appendPathPattern(new PartialPath("root.sg.d2.status"));
    scope.appendPathPattern(new PartialPath("root.sg.d2.a.**"));
    scope.constructTree();

    SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.a.s1"), 0, 0, false, scope);
    checkVisitorResult(visitor, 1, new String[] {"root.sg.d2.a.s1"}, null, new boolean[] {true});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.s1"), 0, 0, false, scope);
    checkVisitorResult(visitor, 0, new String[] {}, null, new boolean[] {});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.s2"), 0, 0, false, scope);
    checkVisitorResult(
        visitor, 2, new String[] {"root.sg.d1.s2", "root.sg.d2.s2"}, new String[] {"", ""}, null);

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.s1"), 0, 0, false, scope);
    checkVisitorResult(visitor, 1, new String[] {"root.sg.d1.s1"}, new String[] {""}, null);

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.status"), 0, 0, false, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        null);

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.*.*"), 0, 0, false, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d1"), 0, 0, true, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.a"), 0, 0, true, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", ""},
        new boolean[] {true, true},
        new int[] {0, 0});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*.*"), 2, 2, false, scope);
    checkVisitorResult(
        visitor,
        1,
        new String[] {"root.sg.d2.s2"},
        new String[] {""},
        new boolean[] {false},
        new int[] {3});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.*"), 2, 3, true, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s2"},
        new String[] {"", ""},
        new boolean[] {true, false},
        new int[] {4, 5});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d1.**"), 0, 0, false, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", ""},
        new boolean[] {false, false});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.d2.**"), 3, 1, true, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s2"},
        new String[] {"", ""},
        new boolean[] {true, false},
        new int[] {2, 3});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.sg.**.status"), 2, 1, true, scope);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s2"},
        new String[] {"status", "status"},
        new boolean[] {true, false},
        new int[] {2, 3});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.*"), 10, 0, false, scope);
    checkVisitorResult(
        visitor,
        5,
        new String[] {
          "root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d2.a.s1", "root.sg.d2.a.s2", "root.sg.d2.s2"
        },
        new String[] {"", "", "", "", ""},
        new boolean[] {false, false, true, true, false},
        new int[] {1, 2, 3, 4, 5});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.**.*.**"), 10, 0, false, scope);
    checkVisitorResult(
        visitor,
        5,
        new String[] {
          "root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d2.a.s1", "root.sg.d2.a.s2", "root.sg.d2.s2"
        },
        new String[] {"", "", "", "", ""},
        new boolean[] {false, false, true, true, false},
        new int[] {1, 2, 3, 4, 5});

    visitor =
        createSchemaTreeVisitorWithLimitOffsetWrapper(
            root, new PartialPath("root.*.**.**"), 10, 0, false, scope);
    checkVisitorResult(
        visitor,
        5,
        new String[] {
          "root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d2.a.s1", "root.sg.d2.a.s2", "root.sg.d2.s2"
        },
        new String[] {"", "", "", "", ""},
        new boolean[] {false, false, true, true, false},
        new int[] {1, 2, 3, 4, 5});
  }

  private void checkVisitorResult(
      SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor,
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
    visitor.close();
  }

  private void checkVisitorResult(
      SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor,
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
    visitor.close();
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
            .map(IMeasurementSchema::getMeasurementName)
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
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toList()));
  }

  @Test
  public void testGetMatchedDevices() throws Exception {
    ISchemaTree schemaTree = new ClusterSchemaTree(generateSchemaTree());

    List<DeviceSchemaInfo> deviceSchemaInfoList =
        schemaTree.getMatchedDevices(new PartialPath("root.sg.d2.a"));
    Assert.assertEquals(1, deviceSchemaInfoList.size());
    DeviceSchemaInfo deviceSchemaInfo = deviceSchemaInfoList.get(0);
    Assert.assertEquals(new PartialPath("root.sg.d2.a"), deviceSchemaInfo.getDevicePath());
    Assert.assertTrue(deviceSchemaInfo.isAligned());
    Assert.assertEquals(2, deviceSchemaInfo.getMeasurements(Sets.newSet("*")).size());

    deviceSchemaInfoList = schemaTree.getMatchedDevices(new PartialPath("root.sg.*"));
    deviceSchemaInfoList.sort(Comparator.comparing(DeviceSchemaInfo::getDevicePath));
    Assert.assertEquals(2, deviceSchemaInfoList.size());
    Assert.assertEquals(new PartialPath("root.sg.d1"), deviceSchemaInfoList.get(0).getDevicePath());
    Assert.assertEquals(new PartialPath("root.sg.d2"), deviceSchemaInfoList.get(1).getDevicePath());

    deviceSchemaInfoList = schemaTree.getMatchedDevices(new PartialPath("root.sg.**"));
    deviceSchemaInfoList.sort(Comparator.comparing(DeviceSchemaInfo::getDevicePath));
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    Assert.assertEquals(new PartialPath("root.sg.d1"), deviceSchemaInfoList.get(0).getDevicePath());
    Assert.assertEquals(new PartialPath("root.sg.d2"), deviceSchemaInfoList.get(1).getDevicePath());
    Assert.assertEquals(
        new PartialPath("root.sg.d2.a"), deviceSchemaInfoList.get(2).getDevicePath());

    deviceSchemaInfoList = schemaTree.getMatchedDevices(new PartialPath("root.**"));
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
  public void testMergeSchemaTreeWithTemplate() throws Exception {
    Template template1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    template1.setId(1);
    ClusterSchemaTree schemaTree1 = new ClusterSchemaTree();
    ClusterSchemaTree schemaTree2 = new ClusterSchemaTree();
    schemaTree1.appendTemplateDevice(new PartialPath("root.sg1.v1.d1"), false, 1, template1);
    schemaTree1.appendTemplateDevice(new PartialPath("root.sg1.v1.d2"), false, 1, template1);
    schemaTree2.appendTemplateDevice(new PartialPath("root.sg1.v1"), false, 1, template1);
    schemaTree1.mergeSchemaTree(schemaTree2);
    List<DeviceSchemaInfo> deviceSchemaInfoList = schemaTree1.getAllDevices();
    Assert.assertEquals(3, deviceSchemaInfoList.size());
    for (DeviceSchemaInfo deviceSchemaInfo : deviceSchemaInfoList) {
      Assert.assertEquals(1, deviceSchemaInfo.getTemplateId());
      int measurementIndex = 1;
      for (MeasurementPath measurementPath : deviceSchemaInfo.getMeasurementSchemaPathList()) {
        Assert.assertEquals("s" + measurementIndex++, measurementPath.getMeasurement());
      }
    }
    Assert.assertFalse(schemaTree1.hasNormalTimeSeries());
    Assert.assertEquals(1, schemaTree1.getUsingTemplates().size());
    checkDeviceUsingTemplate(
        schemaTree1, 1, Sets.newSet("root.sg1.v1.d1", "root.sg1.v1.d2", "root.sg1.v1"));
    Template template2 =
        new Template(
            "t2",
            Arrays.asList("s11", "s22", "s33"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    template2.setId(2);
    ClusterSchemaTree schemaTree3 = new ClusterSchemaTree();
    schemaTree3.appendTemplateDevice(new PartialPath("root.sg2.d1"), false, 2, template2);
    schemaTree1.mergeSchemaTree(schemaTree3);
    Assert.assertFalse(schemaTree1.hasNormalTimeSeries());
    Assert.assertEquals(2, schemaTree1.getUsingTemplates().size());

    checkDeviceUsingTemplate(
        schemaTree1, 1, Sets.newSet("root.sg1.v1.d1", "root.sg1.v1.d2", "root.sg1.v1"));
    checkDeviceUsingTemplate(schemaTree1, 2, Sets.newSet("root.sg2.d1"));
    for (DeviceSchemaInfo deviceSchemaInfo : deviceSchemaInfoList) {
      if (deviceSchemaInfo.getDevicePath().startsWith("root.sg1")) {
        Assert.assertEquals(1, deviceSchemaInfo.getTemplateId());
        int measurementIndex = 1;
        for (MeasurementPath measurementPath : deviceSchemaInfo.getMeasurementSchemaPathList()) {
          Assert.assertEquals("s" + measurementIndex, measurementPath.getMeasurement());
        }
      } else {
        Assert.assertEquals(2, deviceSchemaInfo.getTemplateId());
        int measurementIndex = 1;
        for (MeasurementPath measurementPath : deviceSchemaInfo.getMeasurementSchemaPathList()) {
          Assert.assertEquals(
              String.format("s%d%d", measurementIndex, measurementIndex),
              measurementPath.getMeasurement());
          measurementIndex++;
        }
      }
    }
    ClusterSchemaTree schemaTree4 = new ClusterSchemaTree();
    schemaTree4.appendSingleMeasurementPath(
        new MeasurementPath("root.sg3.d1.s1", TSDataType.INT32));
    schemaTree1.mergeSchemaTree(schemaTree4);
    Assert.assertTrue(schemaTree1.hasNormalTimeSeries());
    Assert.assertEquals(2, schemaTree1.getUsingTemplates().size());
    for (DeviceSchemaInfo deviceSchemaInfo : deviceSchemaInfoList) {
      if (deviceSchemaInfo.getDevicePath().startsWith("root.sg1")) {
        Assert.assertEquals(1, deviceSchemaInfo.getTemplateId());
        int measurementIndex = 1;
        for (MeasurementPath measurementPath : deviceSchemaInfo.getMeasurementSchemaPathList()) {
          Assert.assertEquals("s" + measurementIndex, measurementPath.getMeasurement());
        }
      } else if (deviceSchemaInfo.getDevicePath().startsWith("root.sg2")) {
        Assert.assertEquals(2, deviceSchemaInfo.getTemplateId());
        int measurementIndex = 1;
        for (MeasurementPath measurementPath : deviceSchemaInfo.getMeasurementSchemaPathList()) {
          Assert.assertEquals(
              String.format("s%d%d", measurementIndex, measurementIndex),
              measurementPath.getMeasurement());
          measurementIndex++;
        }
      } else {
        Assert.assertEquals(NON_TEMPLATE, deviceSchemaInfo.getTemplateId());
        Assert.assertEquals(1, deviceSchemaInfo.getMeasurementSchemaPathList().size());
        Assert.assertEquals(
            "s1", deviceSchemaInfo.getMeasurementSchemaPathList().get(0).getMeasurement());
      }
    }
  }

  private void checkDeviceUsingTemplate(
      ISchemaTree schemaTree, int templateId, Set<String> expected) {
    List<PartialPath> deviceUsingTemplate = schemaTree.getDeviceUsingTemplate(templateId);
    Assert.assertEquals(expected.size(), deviceUsingTemplate.size());
    for (PartialPath d : deviceUsingTemplate) {
      Assert.assertTrue(expected.contains(d.getFullPath()));
      expected.remove(d.getFullPath());
    }
    Assert.assertTrue(expected.isEmpty());
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

  protected SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath>
      createSchemaTreeVisitorWithLimitOffsetWrapper(
          SchemaNode root,
          PartialPath pathPattern,
          int slimit,
          int soffset,
          boolean isPrefixMatch) {
    return SchemaTreeVisitorFactory.createSchemaTreeMeasurementVisitor(
        root, pathPattern, isPrefixMatch, slimit, soffset);
  }

  protected SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath>
      createSchemaTreeVisitorWithLimitOffsetWrapper(
          SchemaNode root,
          PartialPath pathPattern,
          int slimit,
          int soffset,
          boolean isPrefixMatch,
          PathPatternTree scope) {
    return SchemaTreeVisitorFactory.createSchemaTreeMeasurementVisitor(
        root, pathPattern, isPrefixMatch, slimit, soffset, scope);
  }

  @Test
  public void testHasView() throws IllegalPathException {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.db.db.s1"),
        new MeasurementSchema("s1", TSDataType.INT32),
        null,
        null,
        null,
        false);
    Assert.assertFalse(schemaTree.hasLogicalViewMeasurement());
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.db.view.s1"),
        new LogicalViewSchema("s1", new TimeSeriesViewOperand("root.db.d.s1")),
        null,
        null,
        null,
        false);
    Assert.assertTrue(schemaTree.hasLogicalViewMeasurement());
  }
}
