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
import java.util.Objects;
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
    if (areAllBranchesFullyPrecise(tagColumnNum, tagDeterminedFilterList)) {
      return tagDeterminedFilterList;
    }

    List<FilterBranch> filterBranches = new ArrayList<>(tagDeterminedFilterList.size());
    final Map<FilterSequenceKey, Integer> suffixSequenceIds = new HashMap<>();
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
      filterBranches.add(new FilterBranch(filterMap, tagColumnNum, suffixSequenceIds));
    }

    final Map<FilterSequenceKey, Integer> prefixSequenceIds = new HashMap<>();
    for (final FilterBranch filterBranch : filterBranches) {
      filterBranch.prefixSequenceId =
          getSequenceId(prefixSequenceIds, 0, filterBranch.filterMap.get(0));
    }
    // A branch group is identified by everything before and after the current tag. Canonical
    // sequence IDs make both the eligibility check and the group-key construction O(1) per branch
    // at each tag, while prefix IDs are updated after earlier precise filters are compacted to IN.
    for (int tagIndex = 1; tagIndex < tagColumnNum; tagIndex++) {
      filterBranches = compactPreciseFiltersAtIndex(filterBranches, tagIndex);
      for (final FilterBranch filterBranch : filterBranches) {
        final List<SchemaFilter> currentFilters = filterBranch.filterMap.get(tagIndex);
        filterBranch.prefixSequenceId =
            getSequenceId(prefixSequenceIds, filterBranch.prefixSequenceId, currentFilters);
        filterBranch.hasNonPrecisePrefix |= !isSinglePreciseTagFilter(currentFilters);
      }
    }

    final List<List<SchemaFilter>> result = new ArrayList<>(filterBranches.size());
    for (final FilterBranch filterBranch : filterBranches) {
      final List<SchemaFilter> filterList = new ArrayList<>();
      filterBranch.filterMap.values().forEach(filterList::addAll);
      result.add(filterList);
    }
    return result;
  }

  private static boolean areAllBranchesFullyPrecise(
      final int tagColumnNum, final List<List<SchemaFilter>> tagDeterminedFilterList) {
    final int[] visitedTagIndexes = new int[tagColumnNum];
    int branchId = 1;
    for (final List<SchemaFilter> tagFilterList : tagDeterminedFilterList) {
      if (tagFilterList.size() != tagColumnNum) {
        return false;
      }
      for (final SchemaFilter schemaFilter : tagFilterList) {
        if (!isPreciseTagFilter(schemaFilter)) {
          return false;
        }
        final int tagIndex = ((TagFilter) schemaFilter).getIndex();
        if (tagIndex < 0 || tagIndex >= tagColumnNum || visitedTagIndexes[tagIndex] == branchId) {
          return false;
        }
        visitedTagIndexes[tagIndex] = branchId;
      }
      branchId++;
    }
    return true;
  }

  private static List<FilterBranch> compactPreciseFiltersAtIndex(
      final List<FilterBranch> filterBranches, final int tagIndex) {
    final Map<FilterBranchKey, List<FilterBranch>> filterGroupMap = new HashMap<>();
    for (final FilterBranch filterBranch : filterBranches) {
      if (!filterBranch.hasNonPrecisePrefix
          || !isSinglePreciseTagFilter(filterBranch.filterMap.get(tagIndex))) {
        continue;
      }
      final FilterBranchKey filterBranchKey =
          new FilterBranchKey(
              filterBranch.prefixSequenceId, filterBranch.suffixSequenceIds[tagIndex]);
      filterGroupMap.computeIfAbsent(filterBranchKey, key -> new ArrayList<>()).add(filterBranch);
    }

    if (filterGroupMap.isEmpty()) {
      return filterBranches;
    }

    int removedBranchCount = 0;
    for (final List<FilterBranch> filterGroup : filterGroupMap.values()) {
      if (filterGroup.size() <= 1) {
        continue;
      }

      final Set<String> preciseValues = new HashSet<>();
      for (final FilterBranch filterBranch : filterGroup) {
        preciseValues.add(
            ((PreciseFilter) ((TagFilter) filterBranch.filterMap.get(tagIndex).get(0)).getChild())
                .getValue());
      }
      if (preciseValues.contains(null)) {
        continue;
      }

      if (preciseValues.size() > 1) {
        filterGroup
            .get(0)
            .filterMap
            .put(
                tagIndex,
                Collections.singletonList(new TagFilter(new InFilter(preciseValues), tagIndex)));
      }
      for (int i = 1; i < filterGroup.size(); i++) {
        filterGroup.get(i).removed = true;
        removedBranchCount++;
      }
    }

    if (removedBranchCount == 0) {
      return filterBranches;
    }

    final List<FilterBranch> result = new ArrayList<>(filterBranches.size() - removedBranchCount);
    for (final FilterBranch filterBranch : filterBranches) {
      if (!filterBranch.removed) {
        result.add(filterBranch);
      }
    }
    return result;
  }

  private static int getSequenceId(
      final Map<FilterSequenceKey, Integer> sequenceIds,
      final int previousSequenceId,
      final List<SchemaFilter> filters) {
    final FilterSequenceKey key = new FilterSequenceKey(previousSequenceId, filters);
    final Integer existingSequenceId = sequenceIds.get(key);
    if (existingSequenceId != null) {
      return existingSequenceId;
    }
    final int newSequenceId = sequenceIds.size() + 1;
    sequenceIds.put(key, newSequenceId);
    return newSequenceId;
  }

  private static boolean isSinglePreciseTagFilter(final List<SchemaFilter> filters) {
    return filters != null && filters.size() == 1 && isPreciseTagFilter(filters.get(0));
  }

  private static boolean isPreciseTagFilter(final SchemaFilter schemaFilter) {
    return schemaFilter.getSchemaFilterType().equals(SchemaFilterType.TAG)
        && ((TagFilter) schemaFilter)
            .getChild()
            .getSchemaFilterType()
            .equals(SchemaFilterType.PRECISE);
  }

  private static final class FilterBranch {

    private final Map<Integer, List<SchemaFilter>> filterMap;
    private final int[] suffixSequenceIds;
    private int prefixSequenceId;
    private boolean hasNonPrecisePrefix;
    private boolean removed;

    private FilterBranch(
        final Map<Integer, List<SchemaFilter>> filterMap,
        final int tagColumnNum,
        final Map<FilterSequenceKey, Integer> suffixSequenceIdMap) {
      this.filterMap = filterMap;
      this.suffixSequenceIds = new int[tagColumnNum];
      this.hasNonPrecisePrefix = !isSinglePreciseTagFilter(filterMap.get(0));

      int suffixSequenceId = 0;
      for (int tagIndex = tagColumnNum - 1; tagIndex >= 0; tagIndex--) {
        suffixSequenceIds[tagIndex] = suffixSequenceId;
        suffixSequenceId =
            getSequenceId(suffixSequenceIdMap, suffixSequenceId, filterMap.get(tagIndex));
      }
    }
  }

  private static final class FilterSequenceKey {

    private final int previousSequenceId;
    private final List<SchemaFilter> filters;

    private FilterSequenceKey(final int previousSequenceId, final List<SchemaFilter> filters) {
      this.previousSequenceId = previousSequenceId;
      this.filters = filters;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FilterSequenceKey)) {
        return false;
      }
      final FilterSequenceKey that = (FilterSequenceKey) o;
      return previousSequenceId == that.previousSequenceId && Objects.equals(filters, that.filters);
    }

    @Override
    public int hashCode() {
      return 31 * previousSequenceId + Objects.hashCode(filters);
    }
  }

  private static final class FilterBranchKey {

    private final int prefixSequenceId;
    private final int suffixSequenceId;

    private FilterBranchKey(final int prefixSequenceId, final int suffixSequenceId) {
      this.prefixSequenceId = prefixSequenceId;
      this.suffixSequenceId = suffixSequenceId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FilterBranchKey)) {
        return false;
      }
      final FilterBranchKey that = (FilterBranchKey) o;
      return prefixSequenceId == that.prefixSequenceId && suffixSequenceId == that.suffixSequenceId;
    }

    @Override
    public int hashCode() {
      return 31 * prefixSequenceId + suffixSequenceId;
    }
  }
}
