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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.parseOptions;

/**
 * {@link DataRegionListeningFilter} is to tell the insertion and deletion for {@link PipeTask} on
 * {@link DataRegion} to collect.
 */
public class DataRegionListeningFilter {

  private static final Set<PartialPath> OPTION_SET = new HashSet<>();

  static {
    try {
      OPTION_SET.add(new PartialPath("data.insert"));
      OPTION_SET.add(new PartialPath("data.delete"));
    } catch (IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  public static boolean shouldDatabaseBeListened(
      final PipeParameters parameters, final boolean isTableModel, final String databaseRawName)
      throws IllegalPathException {
    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        parseInsertionDeletionListeningOptionPair(parameters);
    final boolean hasSpecificListeningOption =
        insertionDeletionListeningOptionPair.getLeft()
            || insertionDeletionListeningOptionPair.getRight();
    if (!hasSpecificListeningOption) {
      return false;
    }

    if (isTableModel) {
      final String databaseTableModel =
          databaseRawName.startsWith("root.") ? databaseRawName.substring(5) : databaseRawName;
      final TablePattern tablePattern =
          TablePattern.parsePipePatternFromSourceParameters(parameters);
      return tablePattern.isTableModelDataAllowedToBeCaptured()
          && tablePattern.matchesDatabase(databaseTableModel);
    } else {
      final String databaseTreeModel =
          databaseRawName.startsWith("root.") ? databaseRawName : "root." + databaseRawName;
      final TreePattern treePattern = TreePattern.parsePipePatternFromSourceParameters(parameters);
      return treePattern.isTreeModelDataAllowedToBeCaptured()
          && treePattern.mayOverlapWithDb(databaseTreeModel);
    }
  }

  public static boolean shouldDataRegionBeListened(
      PipeParameters parameters, DataRegionId dataRegionId) throws IllegalPathException {
    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        parseInsertionDeletionListeningOptionPair(parameters);
    final boolean hasSpecificListeningOption =
        insertionDeletionListeningOptionPair.getLeft()
            || insertionDeletionListeningOptionPair.getRight();
    if (!hasSpecificListeningOption) {
      return false;
    }

    final DataRegion dataRegion = StorageEngine.getInstance().getDataRegion(dataRegionId);
    if (dataRegion == null) {
      return true;
    }

    final String databaseRawName = dataRegion.getDatabaseName();
    final String databaseTreeModel =
        databaseRawName.startsWith("root.") ? databaseRawName : "root." + databaseRawName;
    final String databaseTableModel =
        databaseRawName.startsWith("root.") ? databaseRawName.substring(5) : databaseRawName;

    final TreePattern treePattern = TreePattern.parsePipePatternFromSourceParameters(parameters);
    final TablePattern tablePattern = TablePattern.parsePipePatternFromSourceParameters(parameters);

    return treePattern.isTreeModelDataAllowedToBeCaptured()
            && treePattern.mayOverlapWithDb(databaseTreeModel)
        || tablePattern.isTableModelDataAllowedToBeCaptured()
            && tablePattern.matchesDatabase(databaseTableModel);
  }

  public static Pair<Boolean, Boolean> parseInsertionDeletionListeningOptionPair(
      PipeParameters parameters) throws IllegalPathException, IllegalArgumentException {
    final Set<String> listeningOptions = new HashSet<>();
    final Set<PartialPath> inclusionOptions =
        parseOptions(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE));
    final Set<PartialPath> exclusionOptions =
        parseOptions(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                EXTRACTOR_EXCLUSION_DEFAULT_VALUE));

    inclusionOptions.forEach(
        inclusion ->
            listeningOptions.addAll(
                OPTION_SET.stream()
                    .filter(path -> path.overlapWithFullPathPrefix(inclusion))
                    .map(PartialPath::getFullPath)
                    .collect(Collectors.toSet())));
    exclusionOptions.forEach(
        exclusion ->
            listeningOptions.removeAll(
                OPTION_SET.stream()
                    .filter(path -> path.overlapWithFullPathPrefix(exclusion))
                    .map(PartialPath::getFullPath)
                    .collect(Collectors.toSet())));

    return new Pair<>(
        listeningOptions.contains("data.insert"), listeningOptions.contains("data.delete"));
  }
}
