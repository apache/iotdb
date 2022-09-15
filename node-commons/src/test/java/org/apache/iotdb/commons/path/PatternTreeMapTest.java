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
import org.apache.iotdb.tsfile.read.common.TimeRange;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class PatternTreeMapTest {

  @Test
  public void stringAppendPatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<String> patternTreeMap =
        new PatternTreeMap<>(
            HashSet::new,
            (triggerName, set) -> set.add(triggerName),
            (triggerName, set) -> set.remove(triggerName));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), "A");
    patternTreeMap.append(new PartialPath("root.**.s1"), "B");
    patternTreeMap.append(new PartialPath("root.sg1.*.s1"), "C");
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), "D");
    patternTreeMap.append(new PartialPath("root.sg1.**"), "E");
    patternTreeMap.append(new PartialPath("root.sg1.**.s2"), "F");
    patternTreeMap.append(new PartialPath("root.**.d1.*"), "G");
    patternTreeMap.append(new PartialPath("root.**.d1.**"), "H");
    patternTreeMap.append(new PartialPath("root.*.d1.**"), "I");
    patternTreeMap.append(new PartialPath("root.**"), "J");
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "G", "H", "I", "J")));
    checkOverlapped(
        patternTreeMap, new PartialPath("root.sg2.s1"), new HashSet<>(Arrays.asList("B", "J")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        new HashSet<>(Arrays.asList("E", "F", "G", "H", "I", "J")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        new HashSet<>(Arrays.asList("B", "E", "H", "I", "J")));
    // delete leaf node with common parent
    patternTreeMap.delete(new PartialPath("root.**.d1.*"), "G");
    // only delete value, no delete leaf node
    patternTreeMap.delete(new PartialPath("root.sg1.d1.s1"), "D");
    // delete internal node
    patternTreeMap.delete(new PartialPath("root.**"), "J");
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet<>(Arrays.asList("A", "B", "C", "E", "H", "I")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        new HashSet<>(Arrays.asList("E", "F", "H", "I")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        new HashSet<>(Arrays.asList("B", "E", "H", "I")));
  }

  @Test
  public void timeRangePatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<TimeRange> patternTreeMap =
        new PatternTreeMap<>(
            TreeSet::new,
            (range, set) -> {
              TreeSet<TimeRange> treeSet = (TreeSet) set;
              TimeRange tr = treeSet.floor(range);
              while (tr != null && tr.intersects(range)) {
                range.merge(tr);
                treeSet.remove(tr);
                tr = treeSet.floor(range);
              }
              tr = treeSet.ceiling(range);
              while (tr != null && tr.intersects(range)) {
                range.merge(tr);
                treeSet.remove(tr);
                tr = treeSet.ceiling(range);
              }
              set.add(range);
            },
            null);
    // [1,3] [6,10] [15,20] [22,30]
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(1, 3));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(6, 10));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(15, 20));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(22, 30));

    // [1,20] [22,30]
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(1, 3));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(3, 5));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(8, 12));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(12, 16));
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), new TimeRange(25, 26));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet<>(Arrays.asList(new TimeRange(1, 20), new TimeRange(22, 30))));
  }

  private void checkOverlapped(
      PatternTreeMap patternTreeMap, PartialPath partialPath, Set resultSet) {
    List list = patternTreeMap.getOverlapped(partialPath);
    Assert.assertEquals(resultSet.size(), list.size());
    for (Object o : list) {
      Assert.assertTrue(resultSet.contains(o));
    }
  }
}
