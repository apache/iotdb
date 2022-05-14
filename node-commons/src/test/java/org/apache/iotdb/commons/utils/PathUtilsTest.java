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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class PathUtilsTest {

  @Test
  public void testSplitPathToNodes() throws IllegalPathException {
    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "s1").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.d1.s1"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "s+1").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.d1.`s+1`"));

    assertArrayEquals(
        Arrays.asList("root", "\"s g\"", "d1", "\"s+1\"").toArray(),
        PathUtils.splitPathToDetachedPath("root.`\"s g\"`.d1.`\"s+1\"`"));

    assertArrayEquals(
        Arrays.asList("root", "1").toArray(), PathUtils.splitPathToDetachedPath("root.1"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "s", "1").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.d1.s.1"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "`a.b`").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.d1.```a.b```"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "`").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.d1.````"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "`").toArray(),
        PathUtils.splitPathToDetachedPath("`root`.`sg`.`d1`.````"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1", "`").toArray(),
        PathUtils.splitPathToDetachedPath("`root`.sg.`d1`.````"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "d1.`").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.`d1.```"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "\"d").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.`\\\"d`"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "\td").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.`\\td`"));

    assertArrayEquals(
        Arrays.asList("root", "sg", "\\td").toArray(),
        PathUtils.splitPathToDetachedPath("root.sg.`\\\\td`"));

    assertArrayEquals(
        Arrays.asList("root", "laptop", "d1", "\"1.2.3\"").toArray(),
        PathUtils.splitPathToDetachedPath("root.laptop.d1.`\\\"1.2.3\\\"`"));

    try {
      PathUtils.splitPathToDetachedPath("root.sg.d1.```");
      fail();
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.d1.``` is not a legal path", e.getMessage());
    }

    try {
      PathUtils.splitPathToDetachedPath("root.sg.`d1`..`aa``b`");
      fail();
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.`d1`..`aa``b` is not a legal path", e.getMessage());
    }

    try {
      PathUtils.splitPathToDetachedPath("root.sg.d1.`s+`-1\"`");
      fail();
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.d1.`s+`-1\"` is not a legal path", e.getMessage());
    }

    try {
      PathUtils.splitPathToDetachedPath("root..a");
      fail();
    } catch (IllegalPathException e) {
      Assert.assertEquals("root..a is not a legal path", e.getMessage());
    }

    try {
      PathUtils.splitPathToDetachedPath("root.sg.d1.");
      fail();
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.d1. is not a legal path", e.getMessage());
    }
  }
}
