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
package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.exception.IllegalPathException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class PartialPathTest {

  @Test
  public void testLegalPath() throws IllegalPathException {
    String[] nodes;
    // empty path
    PartialPath a = new PartialPath("", false);
    Assert.assertEquals("", a.getFullPath());
    Assert.assertEquals(0, a.getNodes().length);

    // suffix path
    PartialPath b = new PartialPath("s1");
    Assert.assertEquals("s1", b.getFullPath());
    Assert.assertEquals("s1", b.getNodes()[0]);

    // normal node
    PartialPath c = new PartialPath("root.sg.a");
    Assert.assertEquals("root.sg.a", c.getFullPath());
    nodes = new String[] {"root", "sg", "a"};
    checkNodes(nodes, c.getNodes());

    // quoted node
    PartialPath d = new PartialPath("root.sg.`a.b`");
    Assert.assertEquals("root.sg.`a.b`", d.getFullPath());
    nodes = new String[] {"root", "sg", "`a.b`"};
    checkNodes(nodes, d.getNodes());

    PartialPath e = new PartialPath("root.sg.`a.``b`");
    Assert.assertEquals("root.sg.`a.``b`", e.getFullPath());
    nodes = new String[] {"root", "sg", "`a.``b`"};
    checkNodes(nodes, e.getNodes());

    PartialPath f = new PartialPath("root.`sg\"`.`a.``b`");
    Assert.assertEquals("root.`sg\"`.`a.``b`", f.getFullPath());
    nodes = new String[] {"root", "`sg\"`", "`a.``b`"};
    checkNodes(nodes, f.getNodes());

    PartialPath g = new PartialPath("root.sg.`a.b\\\\`");
    Assert.assertEquals("root.sg.`a.b\\\\`", g.getFullPath());
    nodes = new String[] {"root", "sg", "`a.b\\\\`"};
    checkNodes(nodes, g.getNodes());

    // quoted node of digits
    PartialPath h = new PartialPath("root.sg.`111`");
    Assert.assertEquals("root.sg.`111`", h.getFullPath());
    nodes = new String[] {"root", "sg", "`111`"};
    checkNodes(nodes, h.getNodes());

    // quoted node of key word
    PartialPath i = new PartialPath("root.sg.`select`");
    Assert.assertEquals("root.sg.`select`", i.getFullPath());
    nodes = new String[] {"root", "sg", "`select`"};
    checkNodes(nodes, i.getNodes());

    // wildcard
    PartialPath j = new PartialPath("root.sg.`a*b`");
    Assert.assertEquals("root.sg.`a*b`", j.getFullPath());
    nodes = new String[] {"root", "sg", "`a*b`"};
    checkNodes(nodes, j.getNodes());

    PartialPath k = new PartialPath("root.sg.*");
    Assert.assertEquals("root.sg.*", k.getFullPath());
    nodes = new String[] {"root", "sg", "*"};
    checkNodes(nodes, k.getNodes());

    PartialPath l = new PartialPath("root.sg.**");
    Assert.assertEquals("root.sg.**", l.getFullPath());
    nodes = new String[] {"root", "sg", "**"};
    checkNodes(nodes, l.getNodes());

    // raw key word
    PartialPath m = new PartialPath("root.sg.select");
    Assert.assertEquals("root.sg.select", m.getFullPath());
    nodes = new String[] {"root", "sg", "select"};
    checkNodes(nodes, m.getNodes());

    PartialPath n = new PartialPath("root.sg.device");
    Assert.assertEquals("root.sg.device", n.getFullPath());
    nodes = new String[] {"root", "sg", "device"};
    checkNodes(nodes, n.getNodes());

    PartialPath o = new PartialPath("root.sg.datatype");
    Assert.assertEquals("root.sg.datatype", o.getFullPath());
    nodes = new String[] {"root", "sg", "datatype"};
    checkNodes(nodes, o.getNodes());

    PartialPath r = new PartialPath("root.sg.boolean");
    Assert.assertEquals("root.sg.boolean", r.getFullPath());
    nodes = new String[] {"root", "sg", "boolean"};
    checkNodes(nodes, r.getNodes());

    PartialPath s = new PartialPath("root.sg.DROP_TRIGGER");
    Assert.assertEquals("root.sg.DROP_TRIGGER", s.getFullPath());
    nodes = new String[] {"root", "sg", "DROP_TRIGGER"};
    checkNodes(nodes, s.getNodes());
  }

  @Test
  public void testIllegalPath() {
    try {
      new PartialPath("root.sg.d1.```");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.`d1`..`aa``b`");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1.`s+`-1\"`");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root..a");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1.");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.111");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.and");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.or");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.not");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.contains");
      fail();
    } catch (IllegalPathException ignored) {
    }
  }

  @Test
  public void testLegalDeviceAndMeasurement() throws IllegalPathException {
    String[] nodes;
    // normal node
    PartialPath a = new PartialPath("root.sg", "s1");
    Assert.assertEquals("root.sg.s1", a.getFullPath());
    nodes = new String[] {"root", "sg", "s1"};
    checkNodes(nodes, a.getNodes());

    PartialPath b = new PartialPath("root.sg", "s2");
    Assert.assertEquals("root.sg.s2", b.getFullPath());
    nodes = new String[] {"root", "sg", "s2"};
    checkNodes(nodes, b.getNodes());

    PartialPath c = new PartialPath("root.sg", "a");
    Assert.assertEquals("root.sg.a", c.getFullPath());
    nodes = new String[] {"root", "sg", "a"};
    checkNodes(nodes, c.getNodes());

    // quoted node
    PartialPath d = new PartialPath("root.sg", "`a.b`");
    Assert.assertEquals("root.sg.`a.b`", d.getFullPath());
    nodes = new String[] {"root", "sg", "`a.b`"};
    checkNodes(nodes, d.getNodes());

    PartialPath e = new PartialPath("root.sg", "`a.``b`");
    Assert.assertEquals("root.sg.`a.``b`", e.getFullPath());
    nodes = new String[] {"root", "sg", "`a.``b`"};
    checkNodes(nodes, e.getNodes());

    PartialPath f = new PartialPath("root.`sg\"`", "`a.``b`");
    Assert.assertEquals("root.`sg\"`.`a.``b`", f.getFullPath());
    nodes = new String[] {"root", "`sg\"`", "`a.``b`"};
    checkNodes(nodes, f.getNodes());

    PartialPath g = new PartialPath("root.sg", "`a.b\\\\`");
    Assert.assertEquals("root.sg.`a.b\\\\`", g.getFullPath());
    nodes = new String[] {"root", "sg", "`a.b\\\\`"};
    checkNodes(nodes, g.getNodes());

    // quoted node of digits
    PartialPath h = new PartialPath("root.sg", "`111`");
    Assert.assertEquals("root.sg.`111`", h.getFullPath());
    nodes = new String[] {"root", "sg", "`111`"};
    checkNodes(nodes, h.getNodes());

    // quoted node of key word
    PartialPath i = new PartialPath("root.sg", "`select`");
    Assert.assertEquals("root.sg.`select`", i.getFullPath());
    nodes = new String[] {"root", "sg", "`select`"};
    checkNodes(nodes, i.getNodes());

    // wildcard
    PartialPath j = new PartialPath("root.sg", "`a*b`");
    Assert.assertEquals("root.sg.`a*b`", j.getFullPath());
    nodes = new String[] {"root", "sg", "`a*b`"};
    checkNodes(nodes, j.getNodes());

    PartialPath k = new PartialPath("root.sg", "*");
    Assert.assertEquals("root.sg.*", k.getFullPath());
    nodes = new String[] {"root", "sg", "*"};
    checkNodes(nodes, k.getNodes());

    PartialPath l = new PartialPath("root.sg", "**");
    Assert.assertEquals("root.sg.**", l.getFullPath());
    nodes = new String[] {"root", "sg", "**"};
    checkNodes(nodes, l.getNodes());

    // other
    PartialPath m = new PartialPath("root.sg", "`to`.be.prefix.s");
    Assert.assertEquals("root.sg.`to`.be.prefix.s", m.getFullPath());
    nodes = new String[] {"root", "sg", "`to`", "be", "prefix", "s"};
    checkNodes(nodes, m.getNodes());
  }

  @Test
  public void testIllegalDeviceAndMeasurement() {
    try {
      new PartialPath("root.sg.d1", "```");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1.```", "s1");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.`d1`..a", "`aa``b`");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.`d1`.a", "s..`aa``b`");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1", "`s+`-1\"`");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1.`s+`-1\"`", "s1");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg", "111");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.111", "s1");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.select`", "a");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1", "device`");
      fail();
    } catch (IllegalPathException ignored) {
    }

    try {
      new PartialPath("root.sg.d1.device`", "s1");
      fail();
    } catch (IllegalPathException ignored) {
    }
  }

  @Test
  public void testConcatPath() {
    String[] arr1 = new String[2];
    arr1[0] = "root";
    arr1[1] = "sg1";
    PartialPath a = new PartialPath(arr1);
    String[] arr2 = new String[2];
    arr2[0] = "d1";
    arr2[1] = "s1";
    PartialPath b = new PartialPath(arr2);
    Assert.assertEquals("[root, sg1, d1, s1]", Arrays.toString(a.concatPath(b).getNodes()));
    Assert.assertEquals("s1", b.getTailNode());
    Assert.assertEquals("root.sg1.d1", a.concatPath(b).getDevicePath().getFullPath());
    Assert.assertEquals("root.sg1", a.toString());
  }

  @Test
  public void testConcatArray() throws IllegalPathException {
    PartialPath a = new PartialPath("root", "sg1");
    String[] arr2 = new String[2];
    arr2[0] = "d1";
    arr2[1] = "s1";
    a.concatPath(arr2);
    Assert.assertEquals("[root, sg1, d1, s1]", Arrays.toString(a.getNodes()));
  }

  @Test
  public void testConcatNode() {
    String[] arr1 = new String[2];
    arr1[0] = "root";
    arr1[1] = "sg1";
    PartialPath a = new PartialPath(arr1);
    PartialPath b = a.concatNode("d1");
    Assert.assertEquals("[root, sg1, d1]", Arrays.toString(b.getNodes()));
    Assert.assertEquals("root.sg1.d1", b.getFullPath());
    Assert.assertTrue(b.startsWith(arr1));
    Assert.assertEquals("root", b.getFirstNode());
  }

  @Test
  public void testAlterPrefixPath() throws IllegalPathException {
    // Plain path.
    PartialPath p = new PartialPath("root.a.b.c");
    List<PartialPath> results = p.alterPrefixPath(new PartialPath("root.a.b"));
    Assert.assertEquals(results.toString(), 1, results.size());
    Assert.assertEquals("root.a.b.c", results.get(0).getFullPath());

    // Path with single level wildcard.
    p = new PartialPath("root.*.b.c");
    results = p.alterPrefixPath(new PartialPath("root.a.b"));
    Assert.assertEquals(results.toString(), 1, results.size());
    Assert.assertEquals("root.a.b.c", results.get(0).getFullPath());

    // Path with multi level wildcard.
    p = new PartialPath("root.**.b.c");
    results = p.alterPrefixPath(new PartialPath("root.a.b"));
    Assert.assertEquals(results.toString(), 3, results.size());
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b.c")));
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b.b.c")));
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b.**.b.c")));

    p = new PartialPath("root.**");
    results = p.alterPrefixPath(new PartialPath("root.a.b"));
    Assert.assertEquals(results.toString(), 2, results.size());
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b")));
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b.**")));

    p = new PartialPath("root.**.b.**");
    results = p.alterPrefixPath(new PartialPath("root.a.b.c"));
    Assert.assertEquals(results.toString(), 2, results.size());
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b.c")));
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.a.b.c.**")));

    p = new PartialPath("root.**.b.**.b");
    results = p.alterPrefixPath(new PartialPath("root.b.b.b"));
    Assert.assertEquals(results.toString(), 2, results.size());
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.b.b.b.b")));
    Assert.assertTrue(results.toString(), results.contains(new PartialPath("root.b.b.b.**.b")));

    // Path cannot be altered.
    p = new PartialPath("root.b.c.**");
    results = p.alterPrefixPath(new PartialPath("root.a.b.c"));
    Assert.assertEquals(results.toString(), 0, results.size());
  }

  @Test
  public void testMatchPath() throws IllegalPathException {
    PartialPath p1 = new PartialPath("root.sg1.d1.*");

    Assert.assertTrue(p1.matchFullPath(new PartialPath("root.sg1.d1.s2")));
    Assert.assertFalse(p1.matchFullPath(new PartialPath("root.sg1.d1")));
    Assert.assertFalse(p1.matchFullPath(new PartialPath("root.sg2.d1.*")));
    Assert.assertFalse(p1.matchFullPath(new PartialPath("", false)));

    PartialPath path = new PartialPath("root.sg.d.s");
    String[] patterns = {
      "root.**", "root.**.s", "root.sg.*.s", "root.*.*.*", "root.sg.d.s", "root.s*.d.s"
    };
    for (String pattern : patterns) {
      Assert.assertTrue(new PartialPath(pattern).matchFullPath(path));
    }
  }

  @Test
  public void testPartialPathAndStringList() {
    List<PartialPath> paths =
        PartialPath.fromStringList(Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2"));
    Assert.assertEquals("root.sg1.d1.s1", paths.get(0).getFullPath());
    Assert.assertEquals("root.sg1.d1.s2", paths.get(1).getFullPath());

    List<String> stringPaths = PartialPath.toStringList(paths);
    Assert.assertEquals("root.sg1.d1.s1", stringPaths.get(0));
    Assert.assertEquals("root.sg1.d1.s2", stringPaths.get(1));
  }

  @Test
  public void testOverlapWith() throws IllegalPathException {
    PartialPath[][] pathPairs =
        new PartialPath[][] {
          new PartialPath[] {new PartialPath("root.**"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.**.*"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.**.s"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.*.**"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.*.d.s"), new PartialPath("root.**.s")},
          new PartialPath[] {new PartialPath("root.*.d.s"), new PartialPath("root.sg.*.s")},
          new PartialPath[] {new PartialPath("root.*.d.s"), new PartialPath("root.sg.d2.s")},
          new PartialPath[] {new PartialPath("root.*.d.s.*"), new PartialPath("root.sg.d.s")}
        };
    boolean[] results = new boolean[] {true, true, true, true, true, true, false, false};
    for (int i = 0; i < pathPairs.length; i++) {
      Assert.assertEquals(results[i], pathPairs[i][0].overlapWith(pathPairs[i][1]));
    }
  }

  private void checkNodes(String[] expected, String[] actual) {
    Assert.assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(expected[i], actual[i]);
    }
  }
}
