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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PartialPathTest {
  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
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
    Assert.assertFalse(p1.matchFullPath(new PartialPath("")));

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
}
