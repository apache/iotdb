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
package org.apache.iotdb.tsfile.read.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PathTest {

  private void testPath(Path path, String device, String measurement, String full) {
    assertEquals(device, path.getDevice());
    assertEquals(measurement, path.getMeasurement());
    assertEquals(full, path.getFullPath());
  }

  @Test
  public void construct() throws Exception {
    Path path = new Path("a.b.c");
    testPath(path, "a.b", "c", "a.b.c");
    path = new Path("c");
    testPath(path, "", "c", "c");
    path = new Path("");
    testPath(path, "", "", "");
    // with quote;
    path = new Path("root.d1.r1.\"x1.x2.x3\"");
    testPath(path, "root.d1.r1", "x1.x2.x3", "root.d1.r1.\"x1.x2.x3\"");
  }

  @Test
  public void startWith() throws Exception {
    Path path = new Path("a.b.c");
    assertTrue(path.startWith(new Path("")));
    assertTrue(path.startWith(new Path("a")));
    assertTrue(path.startWith(new Path("a.b.c")));
    path = new Path("a.b.c.\"d.e.f\"");
    assertTrue(path.startWith(new Path("")));
    assertTrue(path.startWith(new Path("a")));
    assertTrue(path.startWith(new Path("a.b.c")));
  }

  @Test
  public void mergePath() throws Exception {
    Path prefix = new Path("a.b.c");
    Path suffix = new Path("d.e");
    Path suffix1 = new Path("");
    Path suffix2 = new Path("d.\"e.f\"");
    testPath(Path.mergePath(prefix, suffix), "a.b.c.d", "e", "a.b.c.d.e");
    testPath(Path.mergePath(prefix, suffix1), "a.b", "c", "a.b.c");
    testPath(Path.mergePath(prefix, suffix2), "a.b.c.d", "e.f", "a.b.c.d.\"e.f\"");
  }

  @Test
  public void addHeadPath() throws Exception {
    Path desc = new Path("a.b.c");
    Path head = new Path("d.e");
    Path head1 = new Path("");
    testPath(Path.addPrefixPath(desc, head), "d.e.a.b", "c", "d.e.a.b.c");
    testPath(Path.mergePath(desc, head1), "a.b", "c", "a.b.c");
  }

  @Test
  public void addHeadPathWithQuote() {
    Path desc = new Path("a.b.\"c\"");
    Path head = new Path("d.e");
    Path head1 = new Path("");
    testPath(Path.addPrefixPath(desc, head), "d.e.a.b", "c", "d.e.a.b.\"c\"");
    testPath(Path.mergePath(desc, head1), "a.b", "c", "a.b.\"c\"");
  }
}