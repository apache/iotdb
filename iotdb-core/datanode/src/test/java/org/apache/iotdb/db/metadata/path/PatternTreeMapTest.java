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
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.StringSerializer;

import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PatternTreeMapTest {

  @Test
  public void stringAppendPatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<String, StringSerializer> patternTreeMap =
        PatternTreeMapFactory.getTriggerPatternTreeMap();

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
    patternTreeMap.append(new PartialPath("root.**.**"), "K");

    checkOverlappedByDevice(
        patternTreeMap,
        new PartialPath("root.sg1.d1"),
        Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        Arrays.asList("A", "B", "C", "D", "E", "G", "H", "I", "J", "K"));
    checkOverlapped(patternTreeMap, new PartialPath("root.sg2.s1"), Arrays.asList("B", "J", "K"));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        Arrays.asList("E", "F", "G", "H", "I", "J", "K"));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        Arrays.asList("B", "E", "H", "I", "J", "K"));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d1"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(
            Arrays.asList("A", "B", "C", "D", "E", "G", "H", "I", "J", "K"),
            Arrays.asList("E", "F", "G", "H", "I", "J", "K")));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d2"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(Arrays.asList("B", "C", "E", "J", "K"), Arrays.asList("E", "F", "J", "K")));
    // delete leaf node with common parent
    patternTreeMap.delete(new PartialPath("root.**.d1.*"), "G");
    // only delete value, no delete leaf node
    patternTreeMap.delete(new PartialPath("root.sg1.d1.s1"), "D");
    // delete internal node
    patternTreeMap.delete(new PartialPath("root.**"), "J");
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        Arrays.asList("A", "B", "C", "E", "H", "I", "K"));
    checkOverlapped(
        patternTreeMap, new PartialPath("root.sg1.d1.s2"), Arrays.asList("E", "F", "H", "I", "K"));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        Arrays.asList("B", "E", "H", "I", "K"));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d1"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(
            Arrays.asList("A", "B", "C", "E", "H", "I", "K"),
            Arrays.asList("E", "F", "H", "I", "K")));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d2"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(Arrays.asList("B", "C", "E", "K"), Arrays.asList("E", "F", "K")));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.v1.d1"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(Arrays.asList("B", "E", "H", "K"), Arrays.asList("E", "F", "H", "K")));
  }

  @Test
  public void modificationPatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<ModEntry, ModsSerializer> patternTreeMap =
        PatternTreeMapFactory.getModsPatternTreeMap();

    // [1,3] [6,10]
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.s1"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s1"), 1, 3));
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.s1"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s1"), 6, 10));

    patternTreeMap.append(
        new PartialPath("root.**.s1"),
        new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 10, 100));
    patternTreeMap.append(
        new PartialPath("root.**.s1"),
        new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 100, 200));

    patternTreeMap.append(
        new PartialPath("root.**"), new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100));
    patternTreeMap.append(
        new PartialPath("root.**"), new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        Arrays.asList(
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s1"), 1, 3),
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s1"), 6, 10),
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 10, 100),
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 100, 200),
            new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100)));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d2.s1"),
        Arrays.asList(
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 10, 100),
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 100, 200),
            new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100)));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        Collections.singletonList(new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100)));

    patternTreeMap.append(
        new PartialPath("root.sg1.d2.s1"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d2.s1"), 4, 6));
    patternTreeMap.append(
        new PartialPath("root.**.s2"),
        new TreeDeletionEntry(new MeasurementPath("root.**.s2"), 4, 6));
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.s3"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s3"), 5, 6));
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.*"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.*"), 4, 6));
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.*.d3.s5"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.*.d3.s5"), 4, 6));
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.*.d3.s4"),
        new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.*.d3.s4"), 4, 6));

    checkOverlappedByDevice(
        patternTreeMap,
        new PartialPath("root.sg1.d1"),
        Arrays.asList(
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s1"), 1, 3),
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s1"), 6, 10),
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 10, 100),
            new TreeDeletionEntry(new MeasurementPath("root.**.s2"), 4, 6),
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.s3"), 5, 6),
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 100, 200),
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.*"), 4, 6),
            new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100)));

    checkOverlappedByDevice(
        patternTreeMap,
        new PartialPath("root.sg1.d1.t1.d3"),
        Arrays.asList(
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 10, 100),
            new TreeDeletionEntry(new MeasurementPath("root.**.s2"), 4, 6),
            new TreeDeletionEntry(new MeasurementPath("root.**.s1"), 100, 200),
            new TreeDeletionEntry(new MeasurementPath("root.**"), 10, 100),
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.*.d3.s5"), 4, 6),
            new TreeDeletionEntry(new MeasurementPath("root.sg1.d1.*.d3.s4"), 4, 6)));
  }

  @Test
  public void testDeviceIdWithNull() {
    PatternTreeMap<ModEntry, ModsSerializer> patternTreeMap =
        PatternTreeMapFactory.getModsPatternTreeMap();
    ModEntry modEntry =
        new TableDeletionEntry(new DeletionPredicate("table1", new NOP()), new TimeRange(0, 100));
    patternTreeMap.append(modEntry.keyOfPatternTree(), modEntry);
    List<ModEntry> result =
        patternTreeMap.getOverlapped(
            Factory.DEFAULT_FACTORY.create(new String[] {"table1", null, "id2"}), "s1");
    assertEquals(Collections.singletonList(modEntry), result);
  }

  private <T> void checkOverlapped(
      PatternTreeMap<T, ?> patternTreeMap, PartialPath partialPath, List<T> expectedList) {
    Set<T> resultSet = new HashSet<>(patternTreeMap.getOverlapped(partialPath));
    Assert.assertEquals(expectedList.size(), resultSet.size());
    for (T o : expectedList) {
      assertTrue(resultSet.contains(o));
    }
  }

  private <T> void checkOverlappedByDeviceMeasurements(
      PatternTreeMap<T, ?> patternTreeMap,
      PartialPath devicePath,
      List<String> measurements,
      List<List<T>> expectedList) {
    List<List<T>> actualList = patternTreeMap.getOverlapped(devicePath, measurements);
    Assert.assertEquals(expectedList.size(), actualList.size());
    for (int i = 0; i < measurements.size(); i++) {
      List<T> expectedSubList = expectedList.get(i);
      Set<T> actualSubSet = new HashSet<>(actualList.get(i));
      Assert.assertEquals(expectedSubList.size(), actualSubSet.size());
      for (T o : expectedSubList) {
        assertTrue(actualSubSet.contains(o));
      }
    }
  }

  private <T> void checkOverlappedByDevice(
      PatternTreeMap<T, ?> patternTreeMap, PartialPath devicePath, List<T> expectedList) {
    Set<T> resultSet = new HashSet<>(patternTreeMap.getDeviceOverlapped(devicePath));
    Assert.assertEquals(expectedList.size(), resultSet.size());
    for (T o : expectedList) {
      assertTrue(resultSet.contains(o));
    }
  }
}
