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
package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.mpp.common.PatternTreeMap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PatternTreeMapTest {

  @Test
  public void stringAppendPatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<String> patternTreeMap =
        new PatternTreeMap<>((triggerName, list) -> list.add(triggerName));
    patternTreeMap.appendPathPattern(new PartialPath("root.sg1.d1.s1"), "A");
    patternTreeMap.appendPathPattern(new PartialPath("root.**.s1"), "B");
    patternTreeMap.appendPathPattern(new PartialPath("root.sg1.*.s1"), "C");
    patternTreeMap.appendPathPattern(new PartialPath("root.sg1.d1.s1"), "D");
    patternTreeMap.appendPathPattern(new PartialPath("root.sg1.**"), "E");
    patternTreeMap.appendPathPattern(new PartialPath("root.sg1.**.s2"), "F");
    patternTreeMap.appendPathPattern(new PartialPath("root.**.d1.*"), "G");
    patternTreeMap.appendPathPattern(new PartialPath("root.**.d1.**"), "H");
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet(Arrays.asList("A", "B", "C", "D", "E", "G", "H")));
    checkOverlapped(
        patternTreeMap, new PartialPath("root.sg2.s1"), new HashSet(Arrays.asList("B")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        new HashSet(Arrays.asList("E", "F", "G", "H")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        new HashSet(Arrays.asList("B", "E", "H")));
  }

  private void checkOverlapped(
      PatternTreeMap patternTreeMap, PartialPath partialPath, Set resultSet) {
    List list = patternTreeMap.getOverlappedPathPatterns(partialPath);
    Assert.assertEquals(resultSet.size(), list.size());
    for (Object o : list) {
      Assert.assertTrue(resultSet.contains(o));
    }
  }

  @Test
  public void modsPatternTreeMapTest() {
    PatternTreeMap<String> patternTreeMap =
        new PatternTreeMap<>((triggerName, list) -> list.add(triggerName));
    PatternTreeMap<Modification> patternTreeMap1 =
        new PatternTreeMap<>(
            (modification, list) -> {
              for (Modification other : list) {
                // Merge modification and other;
              }
              list.clear();
              list.add(modification);
            });
  }
}
