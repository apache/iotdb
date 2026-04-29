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

package org.apache.iotdb.cli.fs.path;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FsPathTest {

  @Test
  public void normalizeAbsolutePath() {
    FsPath path = FsPath.absolute("/root//sg/./d1/../d2/");

    assertEquals("/root/sg/d2", path.toString());
    assertEquals(Arrays.asList("root", "sg", "d2"), path.getSegments());
    assertEquals("d2", path.getFileName());
  }

  @Test
  public void normalizeEmptyPathAsRoot() {
    FsPath path = FsPath.absolute("");

    assertTrue(path.isRoot());
    assertEquals("/", path.toString());
    assertEquals(Collections.emptyList(), path.getSegments());
  }

  @Test
  public void resolveRelativePathAgainstBase() {
    FsPath base = FsPath.absolute("/root/sg/d1");

    assertEquals("/root/sg/d2/s1", base.resolve("../d2/s1").toString());
  }

  @Test
  public void resolveAbsolutePathIgnoresBase() {
    FsPath base = FsPath.absolute("/root/sg/d1");

    assertEquals("/root/other", base.resolve("/root/other").toString());
  }

  @Test
  public void cannotMoveAboveRoot() {
    FsPath path = FsPath.absolute("/root").resolve("../../..");

    assertEquals("/", path.toString());
    assertTrue(path.isRoot());
  }
}
