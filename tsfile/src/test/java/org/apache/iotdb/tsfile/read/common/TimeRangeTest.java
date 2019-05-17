/**
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

import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class TimeRangeTest {

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
     no overlap
   */
  public void getRemainsTest0() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(20, 25));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 10);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), true);
  }

  @Test
  /*
     previous ranges contains current ranges
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
     current ranges contains previous ranges.
     subcase 1
   */
  public void getRemainsTest2() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(1, 3));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 3);
    assertEquals(remainRanges.get(0).getMax(), 10);
    assertEquals(remainRanges.get(0).getLeftClose(), false);
    assertEquals(remainRanges.get(0).getRightClose(), true);
  }

  @Test
  /*
     current ranges contains previous ranges.
     subcase 2
   */
  public void getRemainsTest3() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(5, 10));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 5);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), false);
  }

  @Test
  /*
     current ranges contains previous ranges.
     subcase 3
   */
  public void getRemainsTest4() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(3, 8));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(2, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 3);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), false);
    assertEquals(remainRanges.get(1).getMin(), 8);
    assertEquals(remainRanges.get(1).getMax(), 10);
    assertEquals(remainRanges.get(1).getLeftClose(), false);
    assertEquals(remainRanges.get(1).getRightClose(), true);
  }


  @Test
  /*
     current ranges overlap with previous ranges.
     subcase 1
   */
  public void getRemainsTest5() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(0, 5));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 5);
    assertEquals(remainRanges.get(0).getMax(), 10);
    assertEquals(remainRanges.get(0).getLeftClose(), false);
    assertEquals(remainRanges.get(0).getRightClose(), true);
  }

  @Test
  /*
     current ranges overlap with previous ranges.
     subcase 2
   */
  public void getRemainsTest6() {
    TimeRange r = new TimeRange(3, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(0, 2));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 2);
    assertEquals(remainRanges.get(0).getMax(), 10);
    assertEquals(remainRanges.get(0).getLeftClose(), false);
    assertEquals(remainRanges.get(0).getRightClose(), true);
  }

  @Test
  /*
     current ranges overlap with previous ranges.
     subcase 3
   */
  public void getRemainsTest7() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(6, 12));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 6);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), false);
  }

  @Test
  /*
     current ranges overlap with previous ranges.
     subcase 4
   */
  public void getRemainsTest8() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(11, 13));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(1, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 11);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), false);
  }

  @Test
  /*
     more than one time ranges in previous ranges
   */
  public void getRemainsTest9() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(3, 4));
    prevRanges.add(new TimeRange(6, 8));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(3, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 3);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), false);
    assertEquals(remainRanges.get(1).getMin(), 4);
    assertEquals(remainRanges.get(1).getMax(), 6);
    assertEquals(remainRanges.get(1).getLeftClose(), false);
    assertEquals(remainRanges.get(1).getRightClose(), false);
    assertEquals(remainRanges.get(2).getMin(), 8);
    assertEquals(remainRanges.get(2).getMax(), 10);
    assertEquals(remainRanges.get(2).getLeftClose(), false);
    assertEquals(remainRanges.get(2).getRightClose(), true);
  }

  @Test
  /*
     more than one time ranges in previous ranges
   */
  public void getRemainsTest10() {
    TimeRange r = new TimeRange(1, 10);

    ArrayList<TimeRange> prevRanges = new ArrayList<>();
    prevRanges.add(new TimeRange(3, 4));
    prevRanges.add(new TimeRange(11, 20));

    ArrayList<TimeRange> remainRanges = new ArrayList<>(r.getRemains(prevRanges));
    assertEquals(2, remainRanges.size());
    assertEquals(remainRanges.get(0).getMin(), 1);
    assertEquals(remainRanges.get(0).getMax(), 3);
    assertEquals(remainRanges.get(0).getLeftClose(), true);
    assertEquals(remainRanges.get(0).getRightClose(), false);
    assertEquals(remainRanges.get(1).getMin(), 4);
    assertEquals(remainRanges.get(1).getMax(), 11); // NOTE here is the technical detail.
    assertEquals(remainRanges.get(1).getLeftClose(), false);
    assertEquals(remainRanges.get(1).getRightClose(), false);
  }
}
