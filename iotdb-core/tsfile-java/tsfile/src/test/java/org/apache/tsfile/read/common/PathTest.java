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
package org.apache.tsfile.read.common;

import org.apache.tsfile.exception.PathParseException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.parser.PathVisitor;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PathTest {

  @Test
  public void testDeviceIdWithNull() {
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create(new String[] {"table1", null, "id2"});
    ByteBuffer buffer = ByteBuffer.allocate(128);
    deviceID.serialize(buffer);
    buffer.flip();
    IDeviceID deserialized = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
    assertEquals(deviceID, deserialized);
  }

  @Test
  public void testLegalPath() {
    // empty path
    Path a = new Path("", true);
    Assert.assertEquals(new StringArrayDeviceID(""), a.getIDeviceID());
    Assert.assertEquals("", a.getMeasurement());

    // empty device
    Path b = new Path("s1", true);
    Assert.assertEquals("s1", b.getMeasurement());
    Assert.assertEquals(new StringArrayDeviceID(""), b.getIDeviceID());

    // normal node
    Path c = new Path("root.sg.a", true);
    Assert.assertEquals("root.sg", c.getDeviceString());
    Assert.assertEquals("a", c.getMeasurement());

    // quoted node
    Path d = new Path("root.sg.`a.b`", true);
    Assert.assertEquals("root.sg", d.getDeviceString());
    Assert.assertEquals("`a.b`", d.getMeasurement());

    Path e = new Path("root.sg.`a.``b`", true);
    Assert.assertEquals("root.sg", e.getDeviceString());
    Assert.assertEquals("`a.``b`", e.getMeasurement());

    Path f = new Path("root.`sg\"`.`a.``b`", true);
    Assert.assertEquals("root.`sg\"`", f.getDeviceString());
    Assert.assertEquals("`a.``b`", f.getMeasurement());

    Path g = new Path("root.sg.`a.b\\\\`", true);
    Assert.assertEquals("root.sg", g.getDeviceString());
    Assert.assertEquals("`a.b\\\\`", g.getMeasurement());

    // quoted node of digits
    Path h = new Path("root.sg.`111`", true);
    Assert.assertEquals("root.sg", h.getDeviceString());
    Assert.assertEquals("`111`", h.getMeasurement());

    // quoted node of key word
    Path i = new Path("root.sg.`select`", true);
    Assert.assertEquals("root.sg", i.getDeviceString());
    Assert.assertEquals("select", i.getMeasurement());

    // wildcard
    Path j = new Path("root.sg.`a*b`", true);
    Assert.assertEquals("root.sg", j.getDeviceString());
    Assert.assertEquals("`a*b`", j.getMeasurement());

    Path k = new Path("root.sg.*", true);
    Assert.assertEquals("root.sg", k.getDeviceString());
    Assert.assertEquals("*", k.getMeasurement());

    Path l = new Path("root.sg.**", true);
    Assert.assertEquals("root.sg", l.getDeviceString());
    Assert.assertEquals("**", l.getMeasurement());

    // raw key word
    Path m = new Path("root.sg.select", true);
    Assert.assertEquals("root.sg", m.getDeviceString());
    Assert.assertEquals("select", m.getMeasurement());

    Path n = new Path("root.sg.device", true);
    Assert.assertEquals("root.sg", n.getDeviceString());
    Assert.assertEquals("device", n.getMeasurement());

    Path o = new Path("root.sg.drop_trigger", true);
    Assert.assertEquals("root.sg", o.getDeviceString());
    Assert.assertEquals("drop_trigger", o.getMeasurement());

    Path p = new Path("root.sg.and", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("and", p.getMeasurement());

    p = new Path("root.sg.or", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("or", p.getMeasurement());

    p = new Path("root.sg.not", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("not", p.getMeasurement());

    p = new Path("root.sg.null", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("null", p.getMeasurement());

    p = new Path("root.sg.contains", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("contains", p.getMeasurement());

    p = new Path("root.sg.`0000`", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("`0000`", p.getMeasurement());

    p = new Path("root.sg.`0e38`", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("`0e38`", p.getMeasurement());

    p = new Path("root.sg.`00.12`", true);
    Assert.assertEquals("root.sg", p.getDeviceString());
    Assert.assertEquals("`00.12`", p.getMeasurement());
  }

  @Test
  public void tesIllegalPath() {
    try {
      new Path("root.sg`", true);
      fail();
    } catch (PathParseException ignored) {

    }

    try {
      new Path("root.sg\na", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.select`", true);
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

    try {
      new Path("root.0e38", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.0000", true);
      fail();
    } catch (PathParseException ignored) {
    }
  }

  @Test
  public void testRealNumber() {
    Assert.assertTrue(PathVisitor.isRealNumber("0e38"));
    Assert.assertTrue(PathVisitor.isRealNumber("0000"));
    Assert.assertTrue(PathVisitor.isRealNumber("00.12"));
  }
}
