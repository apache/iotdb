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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class StringContainerTest {

  @Test
  public void testAddTailStringArray() {
    StringContainer a = new StringContainer();
    a.addTail("a", "b", "c");
    a.addTail();
    a.addTail("a", "b", "c");
    assertEquals("abcabc", a.toString());
  }

  @Test
  public void testAddTailStringContainer() {
    StringContainer a = new StringContainer();
    a.addTail("a", "b", "c");
    a.addTail();
    StringContainer b = new StringContainer();
    b.addTail("a", "b", "c");
    a.addTail(b);
    assertEquals("abcabc", a.toString());
  }

  @Test
  public void testAddHeadStringArray() {
    StringContainer a = new StringContainer();
    a.addTail("a", "b", "c");
    StringContainer b = new StringContainer();
    b.addTail("a", "b", "c");
    b.addHead("1", "2", "3");
    a.addHead("!", "@", "#");
    a.addHead(b);
    a.addTail(b);
    assertEquals("123abc!@#abc123abc", a.toString());
  }

  @Test
  public void testAddHeadStringArrayWithSeparator() {
    StringContainer c = new StringContainer(",");
    c.addHead("a", "b", "c");
    assertEquals("a,b,c", c.toString());
    StringContainer a = new StringContainer(",");
    a.addTail("a", "b", "c");
    assertEquals("a,b,c", a.toString());
    StringContainer b = new StringContainer();
    b.addTail("a", "b", "c");
    b.addHead("1", "2", "3");
    a.addHead("!", "@", "#");
    a.addHead(b);
    a.addTail(b);
    assertEquals("1,2,3,a,b,c,!,@,#,a,b,c,1,2,3,a,b,c", a.toString());
  }

  @Test
  public void testGetSubString() {
    StringContainer a = new StringContainer();
    try {
      a.getSubString(0);
    } catch (Exception e) {
      assertTrue(e instanceof IndexOutOfBoundsException);
    }
    a.addHead("a", "bbb", "cc");
    assertEquals("a", a.getSubString(0));
    assertEquals("cc", a.getSubString(-1));
    assertEquals("bbb", a.getSubString(-2));
    try {
      a.getSubString(4);
    } catch (Exception e) {
      assertTrue(e instanceof IndexOutOfBoundsException);
    }
    a.addTail("dd", "eeee");
    assertEquals("a", a.getSubString(0));
    assertEquals("cc", a.getSubString(-3));
    assertEquals("dd", a.getSubString(3));
    assertEquals("eeee", a.getSubString(-1));
    try {
      a.getSubString(9);
    } catch (Exception e) {
      assertTrue(e instanceof IndexOutOfBoundsException);
    }
  }

  @Test
  public void testGetSubStringContainer() {
    StringContainer a = new StringContainer();
    try {
      a.getSubStringContainer(0, 1);
    } catch (Exception e) {
      assertTrue(e instanceof IndexOutOfBoundsException);
    }
    a.addTail("a", "bbb", "cc");
    assertEquals("", a.getSubStringContainer(1, 0).toString());
    assertEquals("a", a.getSubStringContainer(0, 0).toString());
    assertEquals("bbbcc", a.getSubStringContainer(1, -1).toString());
    assertEquals("bbb", a.getSubStringContainer(-2, -2).toString());
    try {
      a.getSubStringContainer(1, 4);
    } catch (Exception e) {
      assertTrue(e instanceof IndexOutOfBoundsException);
    }
    a.addHead("dd", "eeee");
    assertEquals("eeeea", a.getSubStringContainer(1, 2).toString());
    assertEquals("eeeea", a.getSubStringContainer(1, -3).toString());
    assertEquals("dd", a.getSubStringContainer(0, 0).toString());
    assertEquals("cc", a.getSubStringContainer(-1, -1).toString());
    assertEquals("ddeeeeabbbcc", a.getSubStringContainer(-5, -1).toString());
    try {
      a.getSubString(9);
    } catch (Exception e) {
      assertTrue(e instanceof IndexOutOfBoundsException);
    }
  }

  @Test
  public void testEqual() {
    StringContainer c = new StringContainer(",");
    c.addHead("a", "b", "c123");
    c.addTail("a", "12", "c");
    c.addTail("1284736", "b", "c");
    StringContainer copyC = c.clone();
    assertTrue(c.equals(copyC));
    assertFalse(c == copyC);
  }

  @Test
  public void testHashCode() {
    StringContainer c1 = new StringContainer(",");
    c1.addHead("a", "b", "c123");
    c1.addTail("a", "12", "c");
    c1.addTail("1284736", "b", "c");
    StringContainer c2 = new StringContainer(TsFileConstant.PATH_SEPARATOR);
    c2.addHead("a", "b", "c123");
    c2.addTail("a", "12", "c");
    c2.addTail("1284736", "b", "c");
    StringContainer copyC = c1.clone();
    assertEquals(c1.hashCode(), copyC.hashCode());
    assertNotEquals(c1.hashCode(), c2.hashCode());

    StringContainer c3 = new StringContainer(",");
    c3.addHead("a", "b", "c123");
    assertNotEquals(c1.hashCode(), c3.hashCode());

    StringContainer c4 = new StringContainer(",");
    c4.addTail("a", "b", "c123");
    assertNotEquals(c1.hashCode(), c4.hashCode());
  }
}
