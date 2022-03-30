package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaEntityNode;
import org.apache.iotdb.db.mpp.common.schematree.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTreeVisitor;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SchemaTreeTest {

  @Test
  public void testSchemaTreeVisitor() throws Exception {

    SchemaNode root = generateSchemaTree();

    SchemaTreeVisitor visitor =
        new SchemaTreeVisitor(root, new PartialPath("root.sg.d2.a.s1"), 0, 0, false);
    checkVisitorResult(visitor, 1, new String[] {"root.sg.d2.a.s1"}, null, new boolean[] {true});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.s2"), 0, 0, false);
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
        new String[] {"", "status"},
        new boolean[] {true, true});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d1"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", "status"},
        new boolean[] {false, false});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.a"), 0, 0, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s1", "root.sg.d2.a.s2"},
        new String[] {"", "status"},
        new boolean[] {true, true},
        new int[] {0, 0});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*.*"), 2, 2, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"", "status"},
        new boolean[] {false, false},
        new int[] {3, 4});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.*"), 2, 3, true);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1"},
        new String[] {"status", ""},
        new boolean[] {true, false},
        new int[] {4, 5});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d1.**"), 0, 0, false);
    checkVisitorResult(
        visitor,
        2,
        new String[] {"root.sg.d1.s1", "root.sg.d1.s2"},
        new String[] {"", "status"},
        new boolean[] {false, false});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.d2.**"), 3, 1, true);
    checkVisitorResult(
        visitor,
        3,
        new String[] {"root.sg.d2.a.s2", "root.sg.d2.s1", "root.sg.d2.s2"},
        new String[] {"status", "", "status"},
        new boolean[] {true, false, false},
        new int[] {2, 3, 4});

    visitor = new SchemaTreeVisitor(root, new PartialPath("root.sg.**.s2"), 2, 1, true);
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

    MeasurementSchema schema = new MeasurementSchema();
    SchemaMeasurementNode s1 = new SchemaMeasurementNode("s1", schema);
    d1.addChild("s1", s1);
    SchemaMeasurementNode s2 = new SchemaMeasurementNode("s2", schema);
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
}
