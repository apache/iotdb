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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.util;

import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utility class for handling mods operations during TsFile parsing. Supports mods processing logic
 * for both tree model and table model.
 */
public class ModsOperationUtil {

  private ModsOperationUtil() {
    // Utility class, no instantiation allowed
  }

  /**
   * Load all modifications from TsFile and build PatternTreeMap
   *
   * @param tsFile TsFile file
   * @return PatternTreeMap containing all modifications
   */
  public static PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>
      loadModificationsFromTsFile(File tsFile) {
    PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();

    try {
      ModificationFile.readAllModifications(tsFile, true)
          .forEach(
              modification -> modifications.append(modification.keyOfPatternTree(), modification));
    } catch (Exception e) {
      throw new PipeException("Failed to load modifications from TsFile: " + tsFile.getPath(), e);
    }

    return modifications;
  }

  /**
   * Check if data in the specified time range is completely deleted by mods Different logic for
   * tree model and table model
   *
   * @param deviceID device ID
   * @param measurementID measurement ID
   * @param startTime start time
   * @param endTime end time
   * @param modifications modification records
   * @return true if data is completely deleted, false otherwise
   */
  public static boolean isAllDeletedByMods(
      IDeviceID deviceID,
      String measurementID,
      long startTime,
      long endTime,
      PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications) {
    if (modifications == null) {
      return false;
    }

    final List<ModEntry> mods = modifications.getOverlapped(deviceID, measurementID);
    if (mods == null || mods.isEmpty()) {
      return false;
    }

    // Different logic for tree model and table model
    if (deviceID.isTableModel()) {
      // For table model: check if any modification affects the device and covers the time range
      return mods.stream()
          .anyMatch(
              modification ->
                  modification.getTimeRange().contains(startTime, endTime)
                      && modification.affects(deviceID)
                      && modification.affects(measurementID));
    } else {
      // For tree model: check if any modification covers the time range
      return mods.stream()
          .anyMatch(modification -> modification.getTimeRange().contains(startTime, endTime));
    }
  }

  /**
   * Initialize mods mapping for specified measurement list
   *
   * @param deviceID device ID
   * @param measurements measurement list
   * @param modifications modification records
   * @return mapping from measurement ID to mods list and index
   */
  public static List<ModsInfo> initializeMeasurementMods(
      IDeviceID deviceID,
      List<String> measurements,
      PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications) {

    List<ModsInfo> modsInfos = new ArrayList<>(measurements.size());

    for (final String measurement : measurements) {
      final List<ModEntry> mods = modifications.getOverlapped(deviceID, measurement);
      if (mods == null || mods.isEmpty()) {
        // No mods, use empty list and index 0
        modsInfos.add(new ModsInfo(Collections.emptyList(), 0));
        continue;
      }

      // Sort by time range for efficient lookup
      // Different filtering logic for tree model and table model
      final List<ModEntry> filteredMods;
      if (deviceID.isTableModel()) {
        // For table model: filter modifications that affect the device
        filteredMods =
            mods.stream()
                .filter(
                    modification ->
                        modification.affects(deviceID) && modification.affects(measurement))
                .collect(Collectors.toList());
      } else {
        // For tree model: no additional filtering needed
        filteredMods = mods;
      }
      // Store sorted mods and start index
      modsInfos.add(new ModsInfo(ModificationUtils.sortAndMerge(filteredMods), 0));
    }

    return modsInfos;
  }

  /**
   * Check if data at the specified time point is deleted
   *
   * @param time time point
   * @param modsInfo mods information containing mods list and current index
   * @return true if data is deleted, false otherwise
   */
  public static boolean isDelete(long time, ModsInfo modsInfo) {
    if (modsInfo == null) {
      return false;
    }

    final List<ModEntry> mods = modsInfo.getMods();
    if (mods == null || mods.isEmpty()) {
      return false;
    }

    int currentIndex = modsInfo.getCurrentIndex();
    if (currentIndex < 0) {
      return false;
    }

    // First, try to use the current index if it's valid
    if (currentIndex < mods.size()) {
      final ModEntry currentMod = mods.get(currentIndex);
      final long currentModStartTime = currentMod.getTimeRange().getMin();
      final long currentModEndTime = currentMod.getTimeRange().getMax();

      if (time < currentModStartTime) {
        // Time is before current mod, return false
        return false;
      } else if (time <= currentModEndTime) {
        // Time is within current mod range, return true
        return true;
      } else {
        // Time is after current mod, need to search forwards
        return searchAndCheckMod(mods, time, currentIndex + 1, modsInfo);
      }
    } else {
      // Current index is beyond array bounds, all mods have been processed
      clearModsAndReset(modsInfo);
      return false;
    }
  }

  /**
   * Search for a mod using binary search and check if the time point is deleted
   *
   * @param mods sorted list of mods
   * @param time time point to search for
   * @param startIndex starting index for search
   * @param modsInfo mods information to update
   * @return true if data is deleted, false otherwise
   */
  private static boolean searchAndCheckMod(
      List<ModEntry> mods, long time, int startIndex, ModsInfo modsInfo) {
    int searchIndex = binarySearchMods(mods, time, startIndex);
    if (searchIndex >= mods.size()) {
      // All mods checked, clear mods list and reset index to 0
      clearModsAndReset(modsInfo);
      return false;
    }

    final ModEntry foundMod = mods.get(searchIndex);
    final long foundModStartTime = foundMod.getTimeRange().getMin();

    if (time < foundModStartTime) {
      modsInfo.setCurrentIndex(searchIndex);
      return false;
    }

    modsInfo.setCurrentIndex(searchIndex);
    return true;
  }

  /**
   * Clear mods list and reset index to 0
   *
   * @param modsInfo mods information to update
   */
  private static void clearModsAndReset(ModsInfo modsInfo) {
    modsInfo.setMods(Collections.emptyList());
    modsInfo.setCurrentIndex(0);
  }

  /**
   * Binary search to find the first mod that might contain the given time point. Returns the index
   * of the first mod where modStartTime <= time, or mods.size() if no such mod exists.
   *
   * @param mods sorted list of mods
   * @param time time point to search for
   * @param startIndex starting index for search (current index)
   * @return index of the first potential mod, or mods.size() if none found
   */
  private static int binarySearchMods(List<ModEntry> mods, long time, int startIndex) {
    int left = startIndex;
    int right = mods.size();

    while (left < right) {
      int mid = left + (right - left) / 2;
      final long max = mods.get(mid).getTimeRange().getMax();

      if (max < time) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    return left;
  }

  /** Mods information wrapper class, containing mods list and current index */
  public static class ModsInfo {
    private List<ModEntry> mods;
    private int currentIndex;

    public ModsInfo(List<ModEntry> mods, int currentIndex) {
      this.mods = Objects.requireNonNull(mods);
      this.currentIndex = currentIndex;
    }

    public List<ModEntry> getMods() {
      return mods;
    }

    public void setMods(List<ModEntry> newMods) {
      this.mods = newMods;
    }

    public int getCurrentIndex() {
      return currentIndex;
    }

    public void setCurrentIndex(int newIndex) {
      this.currentIndex = newIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ModsInfo modsInfo = (ModsInfo) o;
      return Objects.equals(mods, modsInfo.mods)
          && Objects.equals(currentIndex, modsInfo.currentIndex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mods, currentIndex);
    }

    @Override
    public String toString() {
      return "ModsInfo{" + "mods=" + mods + ", currentIndex=" + currentIndex + '}';
    }
  }
}
