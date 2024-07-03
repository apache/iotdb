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
package org.apache.iotdb.db.schemaengine.schemaregion.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.commons.conf.IoTDBConstant.LOSS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.SDT_PARAMETERS;

public class MetaUtils {

  private MetaUtils() {}

  /**
   * Get database path when creating schema automatically is enabled
   *
   * <p>e.g., path = root.a.b.c and level = 1, return root.a
   *
   * @param path path
   * @param level level
   */
  public static PartialPath getDatabasePathByLevel(PartialPath path, int level)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= level || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(path.getFullPath());
    }
    String[] storageGroupNodes = new String[level + 1];
    System.arraycopy(nodeNames, 0, storageGroupNodes, 0, level + 1);
    return new PartialPath(storageGroupNodes);
  }

  /**
   * PartialPath of aligned time series will be organized to one AlignedPath. BEFORE this method,
   * all the aligned time series is NOT united. For example, given root.sg.d1.vector1[s1] and
   * root.sg.d1.vector1[s2], they will be organized to root.sg.d1.vector1 [s1,s2]
   *
   * @param fullPaths full path list without uniting the sub measurement under the same aligned time
   *     series. The list has been sorted by the alphabetical order, so all the aligned time series
   *     of one device has already been placed contiguously.
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     vector1 (s1,s2) would be returned once.
   * @deprecated
   */
  @Deprecated
  public static List<PartialPath> groupAlignedPaths(List<PartialPath> fullPaths) {
    List<PartialPath> result = new LinkedList<>();
    AlignedPath alignedPath = null;
    for (PartialPath path : fullPaths) {
      MeasurementPath measurementPath = (MeasurementPath) path;
      if (!measurementPath.isUnderAlignedEntity()) {
        result.add(measurementPath);
        alignedPath = null;
      } else {
        if (alignedPath == null
            || !alignedPath.getIDeviceID().equals(measurementPath.getIDeviceID())) {
          alignedPath = new AlignedPath(measurementPath);
          result.add(alignedPath);
        } else {
          alignedPath.addMeasurement(measurementPath);
        }
      }
    }
    return result;
  }

  /**
   * PartialPath of aligned time series will be organized to one AlignedPath. BEFORE this method,
   * all the aligned time series is NOT united. For example, given root.sg.d1.vector1[s1] and
   * root.sg.d1.vector1[s2], they will be organized to root.sg.d1.vector1 [s1,s2]
   *
   * @param fullPaths full path list without uniting the sub measurement under the same aligned time
   *     series. The list has been sorted by the alphabetical order, so all the aligned time series
   *     of one device has already been placed contiguously.
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     vector1 (s1,s2) would be returned once.
   */
  public static List<PartialPath> groupAlignedSeries(List<PartialPath> fullPaths) {
    return groupAlignedSeries(fullPaths, new HashMap<>());
  }

  public static List<PartialPath> groupAlignedSeriesWithOrder(
      List<PartialPath> fullPaths, Ordering timeseriesOrdering) {
    Map<IDeviceID, AlignedPath> deviceToAlignedPathMap = new HashMap<>();
    List<PartialPath> res = groupAlignedSeries(fullPaths, deviceToAlignedPathMap);
    res.sort(
        timeseriesOrdering == Ordering.ASC ? Comparator.naturalOrder() : Comparator.reverseOrder());
    // sort the measurements of AlignedPath
    Comparator<Binary> comparator =
        timeseriesOrdering == Ordering.ASC ? Comparator.naturalOrder() : Comparator.reverseOrder();
    for (AlignedPath alignedPath : deviceToAlignedPathMap.values()) {
      alignedPath.sortMeasurement(comparator);
    }
    return res;
  }

  private static List<PartialPath> groupAlignedSeries(
      List<PartialPath> fullPaths, Map<IDeviceID, AlignedPath> deviceToAlignedPathMap) {
    List<PartialPath> result = new ArrayList<>();
    for (PartialPath path : fullPaths) {
      MeasurementPath measurementPath = (MeasurementPath) path;
      if (!measurementPath.isUnderAlignedEntity()) {
        result.add(measurementPath);
      } else {
        IDeviceID deviceName = measurementPath.getIDeviceID();
        if (!deviceToAlignedPathMap.containsKey(deviceName)) {
          AlignedPath alignedPath = new AlignedPath(measurementPath);
          deviceToAlignedPathMap.put(deviceName, alignedPath);
        } else {
          AlignedPath alignedPath = deviceToAlignedPathMap.get(deviceName);
          alignedPath.addMeasurement(measurementPath);
        }
      }
    }
    result.addAll(deviceToAlignedPathMap.values());
    return result;
  }

  @TestOnly
  public static List<String> getMultiFullPaths(IMNode node) {
    if (node == null) {
      return Collections.emptyList();
    }

    List<IMNode> lastNodeList = new ArrayList<>();
    collectLastNode(node, lastNodeList);

    List<String> result = new ArrayList<>();
    for (IMNode lastNode : lastNodeList) {
      result.add(lastNode.getFullPath());
    }

    return result;
  }

  @TestOnly
  public static void collectLastNode(IMNode node, List<IMNode> lastNodeList) {
    if (node != null) {
      Map<String, IMNode> children = node.getChildren();
      if (children.isEmpty()) {
        lastNodeList.add(node);
      }

      for (Entry<String, IMNode> entry : children.entrySet()) {
        IMNode childNode = entry.getValue();
        collectLastNode(childNode, lastNodeList);
      }
    }
  }

  /**
   * Merge same series and convert to series map. For example: Given: paths: s1, s2, s3, s1 and
   * aggregations: count, sum, count, sum. Then: pathToAggrIndexesMap: s1 -> 0, 3; s2 -> 1; s3 -> 2
   *
   * @param selectedPaths selected series
   * @return path to aggregation indexes map
   */
  public static Map<PartialPath, List<Integer>> groupAggregationsBySeries(
      List<? extends Path> selectedPaths) {
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap = new HashMap<>();
    for (int i = 0; i < selectedPaths.size(); i++) {
      PartialPath series = (PartialPath) selectedPaths.get(i);
      pathToAggrIndexesMap.computeIfAbsent(series, key -> new ArrayList<>()).add(i);
    }
    return pathToAggrIndexesMap;
  }

  /**
   * Group all the series under an aligned entity into one AlignedPath and remove these series from
   * pathToAggrIndexesMap. For example, input map: d1[s1] -> [1, 3], d1[s2] -> [2,4], will return
   * d1[s1,s2], [[1,3], [2,4]]
   */
  public static Map<AlignedPath, List<List<Integer>>> groupAlignedSeriesWithAggregations(
      Map<PartialPath, List<Integer>> pathToAggrIndexesMap) {
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap = new HashMap<>();
    Map<IDeviceID, AlignedPath> temp = new HashMap<>();
    List<PartialPath> seriesPaths = new ArrayList<>(pathToAggrIndexesMap.keySet());
    for (PartialPath seriesPath : seriesPaths) {
      // for with value filter
      if (seriesPath instanceof AlignedPath) {
        List<Integer> indexes = pathToAggrIndexesMap.remove(seriesPath);
        AlignedPath groupPath = temp.get(seriesPath.getIDeviceID());
        if (groupPath == null) {
          groupPath = (AlignedPath) seriesPath.copy();
          temp.put(groupPath.getIDeviceID(), groupPath);
          alignedPathToAggrIndexesMap
              .computeIfAbsent(groupPath, key -> new ArrayList<>())
              .add(indexes);
        } else {
          // groupPath is changed here so we update it
          List<List<Integer>> subIndexes = alignedPathToAggrIndexesMap.remove(groupPath);
          subIndexes.add(indexes);
          groupPath.addMeasurements(((AlignedPath) seriesPath).getMeasurementList());
          groupPath.addSchemas(((AlignedPath) seriesPath).getSchemaList());
          alignedPathToAggrIndexesMap.put(groupPath, subIndexes);
        }
      } else if (((MeasurementPath) seriesPath).isUnderAlignedEntity()) {
        // for without value filter
        List<Integer> indexes = pathToAggrIndexesMap.remove(seriesPath);
        AlignedPath groupPath = temp.get(seriesPath.getIDeviceID());
        if (groupPath == null) {
          groupPath = new AlignedPath((MeasurementPath) seriesPath);
          temp.put(seriesPath.getIDeviceID(), groupPath);
          alignedPathToAggrIndexesMap
              .computeIfAbsent(groupPath, key -> new ArrayList<>())
              .add(indexes);
        } else {
          // groupPath is changed here so we update it
          List<List<Integer>> subIndexes = alignedPathToAggrIndexesMap.remove(groupPath);
          subIndexes.add(indexes);
          groupPath.addMeasurement((MeasurementPath) seriesPath);
          alignedPathToAggrIndexesMap.put(groupPath, subIndexes);
        }
      }
    }
    return alignedPathToAggrIndexesMap;
  }

  public static Map<PartialPath, List<AggregationDescriptor>> groupAlignedAggregations(
      Map<PartialPath, List<AggregationDescriptor>> pathToAggregations) {
    Map<PartialPath, List<AggregationDescriptor>> result = new HashMap<>();
    Map<String, List<MeasurementPath>> deviceToAlignedPathsMap = new HashMap<>();
    for (PartialPath path : pathToAggregations.keySet()) {
      MeasurementPath measurementPath = (MeasurementPath) path;
      if (!measurementPath.isUnderAlignedEntity()) {
        result
            .computeIfAbsent(measurementPath, key -> new ArrayList<>())
            .addAll(pathToAggregations.get(path));
      } else {
        deviceToAlignedPathsMap
            .computeIfAbsent(path.getIDeviceID().toString(), key -> new ArrayList<>())
            .add(measurementPath);
      }
    }
    for (Map.Entry<String, List<MeasurementPath>> alignedPathEntry :
        deviceToAlignedPathsMap.entrySet()) {
      List<MeasurementPath> measurementPathList = alignedPathEntry.getValue();
      AlignedPath alignedPath = null;
      List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
      for (int i = 0; i < measurementPathList.size(); i++) {
        MeasurementPath measurementPath = measurementPathList.get(i);
        if (i == 0) {
          alignedPath = new AlignedPath(measurementPath);
        } else {
          alignedPath.addMeasurement(measurementPath);
        }
        aggregationDescriptorList.addAll(pathToAggregations.get(measurementPath));
      }
      result.put(alignedPath, aggregationDescriptorList);
    }
    return result;
  }

  public static Pair<String, String> parseDeadbandInfo(Map<String, String> props) {
    if (props == null) {
      return new Pair<>(null, null);
    }
    String deadband = props.get(LOSS);
    deadband = deadband == null ? null : deadband.toUpperCase(Locale.ROOT);
    Map<String, String> deadbandParameters = new HashMap<>();
    for (String k : SDT_PARAMETERS) {
      if (props.containsKey(k)) {
        deadbandParameters.put(k, props.get(k));
      }
    }

    return new Pair<>(
        deadband, deadbandParameters.isEmpty() ? null : String.format("%s", deadbandParameters));
  }
}
