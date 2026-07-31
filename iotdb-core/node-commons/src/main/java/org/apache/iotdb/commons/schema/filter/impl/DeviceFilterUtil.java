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

package org.apache.iotdb.commons.schema.filter.impl;

import org.apache.iotdb.commons.i18n.SchemaMessages;
import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.TagFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class DeviceFilterUtil {

  private DeviceFilterUtil() {
    // do nothing
  }

  // if the element in idDeterminedFilterList isEmpty, the corresponding pattern is
  // root.db.table.*.*..
  // e.g. input (db, table[c1, c2], [[]]), return [root.db.table.*.*]
  public static List<PartialPath> convertToDevicePattern(
      final String[] prefix,
      final int tagColumnNum,
      final List<List<SchemaFilter>> tagDeterminedFilterList,
      final boolean isRestrict) {
    final List<PartialPath> pathList = new ArrayList<>();
    final int length = tagColumnNum + prefix.length;
    for (final List<SchemaFilter> tagFilterList :
        compactNonLeadingPreciseFilters(tagColumnNum, tagDeterminedFilterList)) {
      final String[] nodes = new String[length];
      Arrays.fill(nodes, ONE_LEVEL_PATH_WILDCARD);
      System.arraycopy(prefix, 0, nodes, 0, prefix.length);
      final ExtendedPartialPath partialPath = new ExtendedPartialPath(nodes, isRestrict);
      for (final SchemaFilter schemaFilter : tagFilterList) {
        if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.TAG)) {
          final int index = ((TagFilter) schemaFilter).getIndex() + prefix.length;
          final SchemaFilter childFilter = ((TagFilter) schemaFilter).getChild();
          if (childFilter.getSchemaFilterType().equals(SchemaFilterType.PRECISE)) {
            // If there is a precise filter, other filters on the same id are processed and thus
            // not exist here
            nodes[index] = ((PreciseFilter) childFilter).getValue();
          } else if (childFilter.getSchemaFilterType().equals(SchemaFilterType.IN)) {
            partialPath.addMultiExactMatch(index, ((InFilter) childFilter).getValues());
          } else {
            partialPath.addMatchFunction(
                index,
                node ->
                    Boolean.TRUE.equals(
                        childFilter.accept(StringValueFilterVisitor.getInstance(), node)));
          }
        } else {
          throw new IllegalStateException(
              SchemaMessages.INPUT_SINGLE_FILTER_MUST_BE_DEVICE_ID_FILTER);
        }
      }
      pathList.add(partialPath);
    }
    return pathList;
  }

  /**
   * IN predicates and OR-connected equal predicates are expanded into precise filter branches so
   * that complete device IDs can use the device cache. If a precise filter is behind an unfixed tag
   * level, however, every expanded branch traverses the same wildcard subtree. Compact branches
   * that differ only at such a tag into one IN filter to avoid repeated schema traversals.
   */
  private static List<List<SchemaFilter>> compactNonLeadingPreciseFilters(
      final int tagColumnNum, final List<List<SchemaFilter>> tagDeterminedFilterList) {
    if (tagColumnNum <= 1 || tagDeterminedFilterList.size() <= 1) {
      return tagDeterminedFilterList;
    }

    List<Map<Integer, List<SchemaFilter>>> filterMaps =
        new ArrayList<>(tagDeterminedFilterList.size());
    for (final List<SchemaFilter> tagFilterList : tagDeterminedFilterList) {
      final Map<Integer, List<SchemaFilter>> filterMap = new TreeMap<>();
      for (final SchemaFilter schemaFilter : tagFilterList) {
        if (!schemaFilter.getSchemaFilterType().equals(SchemaFilterType.TAG)) {
          throw new IllegalStateException(
              SchemaMessages.INPUT_SINGLE_FILTER_MUST_BE_DEVICE_ID_FILTER);
        }
        final int tagIndex = ((TagFilter) schemaFilter).getIndex();
        filterMap.computeIfAbsent(tagIndex, key -> new ArrayList<>()).add(schemaFilter);
      }
      filterMaps.add(filterMap);
    }

    for (int tagIndex = 1; tagIndex < tagColumnNum; tagIndex++) {
      filterMaps = compactPreciseFiltersAtIndex(filterMaps, tagIndex);
    }

    final List<List<SchemaFilter>> result = new ArrayList<>(filterMaps.size());
    for (final Map<Integer, List<SchemaFilter>> filterMap : filterMaps) {
      final List<SchemaFilter> filterList = new ArrayList<>();
      filterMap.values().forEach(filterList::addAll);
      result.add(filterList);
    }
    return result;
  }

  private static List<Map<Integer, List<SchemaFilter>>> compactPreciseFiltersAtIndex(
      final List<Map<Integer, List<SchemaFilter>>> filterMaps, final int tagIndex) {
    final Map<Map<Integer, List<SchemaFilter>>, List<Integer>> filterGroupMap = new HashMap<>();
    for (int i = 0; i < filterMaps.size(); i++) {
      final Map<Integer, List<SchemaFilter>> filterMap = filterMaps.get(i);
      if (!canCompactAtIndex(filterMap, tagIndex)) {
        continue;
      }
      final Map<Integer, List<SchemaFilter>> filtersExceptCurrentIndex = new TreeMap<>(filterMap);
      filtersExceptCurrentIndex.remove(tagIndex);
      filterGroupMap.computeIfAbsent(filtersExceptCurrentIndex, key -> new ArrayList<>()).add(i);
    }

    if (filterGroupMap.isEmpty()) {
      return filterMaps;
    }

    final Map<Integer, Map<Integer, List<SchemaFilter>>> compactedFilterMap = new HashMap<>();
    final Set<Integer> removedIndexes = new HashSet<>();
    for (final List<Integer> groupIndexes : filterGroupMap.values()) {
      if (groupIndexes.size() <= 1) {
        continue;
      }

      final Set<String> preciseValues = new HashSet<>();
      for (final int index : groupIndexes) {
        preciseValues.add(
            ((PreciseFilter) ((TagFilter) filterMaps.get(index).get(tagIndex).get(0)).getChild())
                .getValue());
      }
      if (preciseValues.contains(null)) {
        continue;
      }

      final int firstIndex = groupIndexes.get(0);
      final Map<Integer, List<SchemaFilter>> mergedFilterMap =
          new TreeMap<>(filterMaps.get(firstIndex));
      if (preciseValues.size() > 1) {
        mergedFilterMap.put(
            tagIndex,
            Collections.singletonList(new TagFilter(new InFilter(preciseValues), tagIndex)));
      }
      compactedFilterMap.put(firstIndex, mergedFilterMap);
      removedIndexes.addAll(groupIndexes.subList(1, groupIndexes.size()));
    }

    if (compactedFilterMap.isEmpty()) {
      return filterMaps;
    }

    final List<Map<Integer, List<SchemaFilter>>> result = new ArrayList<>(filterMaps.size());
    for (int i = 0; i < filterMaps.size(); i++) {
      if (compactedFilterMap.containsKey(i)) {
        result.add(compactedFilterMap.get(i));
      } else if (!removedIndexes.contains(i)) {
        result.add(filterMaps.get(i));
      }
    }
    return result;
  }

  private static boolean canCompactAtIndex(
      final Map<Integer, List<SchemaFilter>> filterMap, final int tagIndex) {
    final List<SchemaFilter> currentFilters = filterMap.get(tagIndex);
    if (currentFilters == null
        || currentFilters.size() != 1
        || !isPreciseTagFilter(currentFilters.get(0))) {
      return false;
    }

    for (int i = 0; i < tagIndex; i++) {
      final List<SchemaFilter> precedingFilters = filterMap.get(i);
      if (precedingFilters == null
          || precedingFilters.size() != 1
          || !isPreciseTagFilter(precedingFilters.get(0))) {
        return true;
      }
    }
    return false;
  }

  private static boolean isPreciseTagFilter(final SchemaFilter schemaFilter) {
    return schemaFilter.getSchemaFilterType().equals(SchemaFilterType.TAG)
        && ((TagFilter) schemaFilter)
            .getChild()
            .getSchemaFilterType()
            .equals(SchemaFilterType.PRECISE);
  }
}
