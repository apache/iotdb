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

package org.apache.iotdb.db.pipe.extractor.dataregion;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.datastructure.PipeInclusionNormalizer.getPartialPaths;

/**
 * {@link PipeDataRegionFilter} is to tell the insertion and deletion for {@link PipeTask} on {@link
 * DataRegion} to collect.
 */
class PipeDataRegionFilter {

  private static final Set<PartialPath> TYPE_SET = new HashSet<>();

  static {
    try {
      TYPE_SET.add(new PartialPath("data.insert"));
      TYPE_SET.add(new PartialPath("data.delete"));
    } catch (IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  static Pair<Boolean, Boolean> getDataRegionListenPair(String inclusionStr, String exclusionStr)
      throws IllegalPathException, IllegalArgumentException {
    Set<String> listenTypes = new HashSet<>();
    List<PartialPath> inclusionPath = getPartialPaths(inclusionStr);
    List<PartialPath> exclusionPath = getPartialPaths(exclusionStr);
    inclusionPath.forEach(
        inclusion ->
            listenTypes.addAll(
                TYPE_SET.stream()
                    .filter(path -> path.overlapWithFullPathPrefix(inclusion))
                    .map(PartialPath::getFullPath)
                    .collect(Collectors.toSet())));
    exclusionPath.forEach(
        exclusion ->
            listenTypes.removeAll(
                TYPE_SET.stream()
                    .filter(path -> path.overlapWithFullPathPrefix(exclusion))
                    .map(PartialPath::getFullPath)
                    .collect(Collectors.toSet())));

    return new Pair<>(listenTypes.contains("data.insert"), listenTypes.contains("data.delete"));
  }
}
