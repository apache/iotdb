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

import org.apache.iotdb.tsfile.exception.PathParseException;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class PathTest {
  @Test
  public void testLegalPath() {
    // empty path
    Path a = new Path("", true);
    Assert.assertEquals("", a.getDeviceIdString());
    Assert.assertEquals("", a.getMeasurement());

    // empty device
    Path b = new Path("s1", true);
    Assert.assertEquals("s1", b.getMeasurement());
    Assert.assertEquals("", b.getDeviceIdString());

    // normal node
    Path c = new Path("root.sg.a", true);
    Assert.assertEquals("root.sg", c.getDeviceIdString());
    Assert.assertEquals("a", c.getMeasurement());

    // quoted node
    Path d = new Path("root.sg.`a.b`", true);
    Assert.assertEquals("root.sg", d.getDeviceIdString());
    Assert.assertEquals("`a.b`", d.getMeasurement());

    Path e = new Path("root.sg.`a.``b`", true);
    Assert.assertEquals("root.sg", e.getDeviceIdString());
    Assert.assertEquals("`a.``b`", e.getMeasurement());

    Path f = new Path("root.`sg\"`.`a.``b`", true);
    Assert.assertEquals("root.`sg\"`", f.getDeviceIdString());
    Assert.assertEquals("`a.``b`", f.getMeasurement());

    Path g = new Path("root.sg.`a.b\\\\`", true);
    Assert.assertEquals("root.sg", g.getDeviceIdString());
    Assert.assertEquals("`a.b\\\\`", g.getMeasurement());

    // quoted node of digits
    Path h = new Path("root.sg.`111`", true);
    Assert.assertEquals("root.sg", h.getDeviceIdString());
    Assert.assertEquals("`111`", h.getMeasurement());

    // quoted node of key word
    Path i = new Path("root.sg.`select`", true);
    Assert.assertEquals("root.sg", i.getDeviceIdString());
    Assert.assertEquals("`select`", i.getMeasurement());

    // wildcard
    Path j = new Path("root.sg.`a*b`", true);
    Assert.assertEquals("root.sg", j.getDeviceIdString());
    Assert.assertEquals("`a*b`", j.getMeasurement());

    Path k = new Path("root.sg.*", true);
    Assert.assertEquals("root.sg", k.getDeviceIdString());
    Assert.assertEquals("*", k.getMeasurement());

    Path l = new Path("root.sg.**", true);
    Assert.assertEquals("root.sg", l.getDeviceIdString());
    Assert.assertEquals("**", l.getMeasurement());
  }

  @Test
  public void tesIllegalPath() {
    try {
      new Path("root.sg`", true);
      fail();
    } catch (PathParseException ignored) {

    }

    try {
      // key word with out `
      new Path("root.select", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      // pure digits
      new Path("root.111", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      // single ` in quoted node
      new Path("root.`a``", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      // single ` in quoted node
      new Path("root.``a`", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.a*%", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.a*b", true);
      fail();
    } catch (PathParseException ignored) {
    }

    //    try {
    //      Path path = new Path("root.*ab*", true);
    //      fail();
    //    } catch (PathParseException ignored) {
    //    }
  }
}
