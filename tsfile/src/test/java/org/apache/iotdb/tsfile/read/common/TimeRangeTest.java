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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeRangeTest {

  @Test
  /*
   * [1,3] does not intersect with (4,5].
   */
  public void intersect1() {
    TimeRange r1 = new TimeRange(1, 3);
    TimeRange r2 = new TimeRange(4, 5);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 3 ]", r1.toString());
    assertEquals("( 4 : 5 ]", r2.toString());
    assertFalse(r1.intersects(r2));
    assertFalse(r2.intersects(r1));
  }

  @Test
  /*
   * [1,3) does not intersect with (3,5]
   */
  public void intersect2() {
    TimeRange r1 = new TimeRange(1, 3);
    r1.setRightClose(false);
    TimeRange r2 = new TimeRange(3, 5);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 3 )", r1.toString());
    assertEquals("( 3 : 5 ]", r2.toString());
    assertFalse(r1.intersects(r2));
    assertFalse(r2.intersects(r1));
  }

  @Test
  /*
   * [1,3] does not intersect with [5,6].
   */
  public void intersect3() {
    TimeRange r1 = new TimeRange(1, 3);
    TimeRange r2 = new TimeRange(5, 6);
    assertEquals("[ 1 : 3 ]", r1.toString());
    assertEquals("[ 5 : 6 ]", r2.toString());
    assertFalse(r1.intersects(r2));
    assertFalse(r2.intersects(r1));
  }

  @Test
  /*
   * [1,3] intersects with [2,5].
   */
  public void intersect4() {
    TimeRange r1 = new TimeRange(1, 3);
    TimeRange r2 = new TimeRange(2, 5);
    assertEquals("[ 1 : 3 ]", r1.toString());
    assertEquals("[ 2 : 5 ]", r2.toString());
    assertTrue(r1.intersects(r2));
    assertTrue(r2.intersects(r1));
  }

  @Test
  /*
   * [1,3] intersects with (3,5].
   */
  public void intersect5() {
    TimeRange r1 = new TimeRange(1, 3);
    TimeRange r2 = new TimeRange(3, 5);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 3 ]", r1.toString());
    assertEquals("( 3 : 5 ]", r2.toString());
    assertTrue(r1.intersects(r2));
    assertTrue(r2.intersects(r1));
  }

  @Test
  /*
   * [1,3) intersects with (2,5].
   */
  public void intersect6() {
    TimeRange r1 = new TimeRange(1, 3);
    r1.setRightClose(false);
    TimeRange r2 = new TimeRange(2, 5);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 3 )", r1.toString());
    assertEquals("( 2 : 5 ]", r2.toString());
    assertTrue(r1.intersects(r2));
    assertTrue(r2.intersects(r1));
  }

  @Test
  public void overlap1() {
    TimeRange r1 = new TimeRange(1, 4);
    TimeRange r2 = new TimeRange(4, 5);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 4 ]", r1.toString());
    assertEquals("( 4 : 5 ]", r2.toString());
    assertFalse(r1.overlaps(r2));
  }

  @Test
  public void overlap2() {
    TimeRange r1 = new TimeRange(1, 4);
    r1.setRightClose(false);
    TimeRange r2 = new TimeRange(3, 6);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 4 )", r1.toString());
    assertEquals("( 3 : 6 ]", r2.toString());
    assertFalse(r1.overlaps(r2));
  }

  @Test
  /*
   * [1,3] does not intersect with [5,6].
   */
  public void overlap3() {
    TimeRange r1 = new TimeRange(1, 4);
    TimeRange r2 = new TimeRange(5, 8);
    assertEquals("[ 1 : 4 ]", r1.toString());
    assertEquals("[ 5 : 8 ]", r2.toString());
    assertFalse(r1.overlaps(r2));
  }

  @Test
  public void overlap4() {
    TimeRange r1 = new TimeRange(1, 4);
    TimeRange r2 = new TimeRange(2, 5);
    assertEquals("[ 1 : 4 ]", r1.toString());
    assertEquals("[ 2 : 5 ]", r2.toString());
    assertTrue(r1.overlaps(r2));
  }

  @Test
  public void overlap5() {
    TimeRange r1 = new TimeRange(1, 4);
    TimeRange r2 = new TimeRange(3, 5);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 4 ]", r1.toString());
    assertEquals("( 3 : 5 ]", r2.toString());
    assertTrue(r1.overlaps(r2));
  }

  @Test
  public void overlap6() {
    TimeRange r1 = new TimeRange(1, 5);
    r1.setRightClose(false);
    TimeRange r2 = new TimeRange(2, 6);
    r2.setLeftClose(false);
    assertEquals("[ 1 : 5 )", r1.toString());
    assertEquals("( 2 : 6 ]", r2.toString());
    assertTrue(r1.overlaps(r2));
  }

  @Test
  public void equalTest() {
    TimeRange r1 = new TimeRange(5, 8);
    TimeRange r2 = new TimeRange(5, 8);
    assertTrue(r1.equals(r2));
  }

  @Test
  public void mergeTest() {
    ArrayList<TimeRange> unionCandidates = new ArrayList<>();
    unionCandidates.add(new TimeRange(0L, 10L));
    unionCandidates.add(new TimeRange(3L, 10L));
    unionCandidates.add(new TimeRange(100L, 200L));
    unionCandidates.add(new TimeRange(20L, 30L));
    unionCandidates.add(new TimeRange(5L, 6L));

    ArrayList<TimeRange> res = new ArrayList<>(TimeRange.sortAndMerge(unionCandidates));

    Assert.assertEquals(3, res.size());
    Assert.assertEquals("[ 0 : 10 ]", res.get(0).toString());
    Assert.assertEquals("[ 20 : 30 ]", res.get(1).toString());
    Assert.assertEquals("[ 100 : 200 ]", res.get(2).toString());
  }

  @Test
  /*
   * no overlap
   */
  public void getRemainsTest0() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(20, 25));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(10, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertTrue(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * previous ranges contains current ranges
   */
  public void getRemainsTest1() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(0, 10));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(0, remainRanges.size());
  }

  @Test
  /*
   * current ranges contains previous ranges. subcase 1
   */
  public void getRemainsTest2() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(1, 3));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(3, remainRanges.get(0).getMin());
    assertEquals(10, remainRanges.get(0).getMax());
    assertFalse(remainRanges.get(0).getLeftClose());
    assertTrue(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * current ranges contains previous ranges. subcase 2
   */
  public void getRemainsTest3() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(5, 10));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(5, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertFalse(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * current ranges contains previous ranges. subcase 3
   */
  public void getRemainsTest4() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(3, 8));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(2, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(3, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertFalse(remainRanges.get(0).getRightClose());
    assertEquals(8, remainRanges.get(1).getMin());
    assertEquals(10, remainRanges.get(1).getMax());
    assertFalse(remainRanges.get(1).getLeftClose());
    assertTrue(remainRanges.get(1).getRightClose());
  }

  @Test
  /*
   * current ranges overlap with previous ranges. subcase 1
   */
  public void getRemainsTest5() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(0, 5));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(5, remainRanges.get(0).getMin());
    assertEquals(10, remainRanges.get(0).getMax());
    assertFalse(remainRanges.get(0).getLeftClose());
    assertTrue(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * current ranges overlap with previous ranges. subcase 2
   */
  public void getRemainsTest6() {
    TimeRange r = new TimeRange(3, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(0, 2));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(2, remainRanges.get(0).getMin());
    assertEquals(10, remainRanges.get(0).getMax());
    assertFalse(remainRanges.get(0).getLeftClose());
    assertTrue(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * current ranges overlap with previous ranges. subcase 3
   */
  public void getRemainsTest7() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(6, 12));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(6, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertFalse(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * current ranges overlap with previous ranges. subcase 4
   */
  public void getRemainsTest8() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(11, 13));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(11, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertFalse(remainRanges.get(0).getRightClose());
  }

  @Test
  /*
   * more than one time ranges in previous ranges
   */
  public void getRemainsTest9() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(3, 4));
    prevRanges.add(new TimeRange(6, 8));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(3, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(3, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertFalse(remainRanges.get(0).getRightClose());
    assertEquals(4, remainRanges.get(1).getMin());
    assertEquals(6, remainRanges.get(1).getMax());
    assertFalse(remainRanges.get(1).getLeftClose());
    assertFalse(remainRanges.get(1).getRightClose());
    assertEquals(8, remainRanges.get(2).getMin());
    assertEquals(10, remainRanges.get(2).getMax());
    assertFalse(remainRanges.get(2).getLeftClose());
    assertTrue(remainRanges.get(2).getRightClose());
  }

  @Test
  /*
   * more than one time ranges in previous ranges
   */
  public void getRemainsTest10() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(3, 4));
    prevRanges.add(new TimeRange(11, 20));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(2, remainRanges.size());
    assertEquals(1, remainRanges.get(0).getMin());
    assertEquals(3, remainRanges.get(0).getMax());
    assertTrue(remainRanges.get(0).getLeftClose());
    assertFalse(remainRanges.get(0).getRightClose());
    assertEquals(4, remainRanges.get(1).getMin());
    assertEquals(11, remainRanges.get(1).getMax()); // NOTE here is the technical detail.
    assertFalse(remainRanges.get(1).getLeftClose());
    assertFalse(remainRanges.get(1).getRightClose());
  }

  @Test
  /*
   * current ranges DO NOT overlap with previous ranges.
   */
  public void getRemainsTest11() {
    TimeRange r = new TimeRange(4, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(1, 2));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(4, remainRanges.get(0).getMin());
    assertEquals(10, remainRanges.get(0).getMax());
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), true);
  }
}
