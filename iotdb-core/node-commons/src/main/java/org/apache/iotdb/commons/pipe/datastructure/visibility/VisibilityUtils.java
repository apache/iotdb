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

package org.apache.iotdb.commons.pipe.datastructure.visibility;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

public class VisibilityUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(VisibilityUtils.class);

  private VisibilityUtils() {
    // forbidding instantiation
  }

  public static boolean isCompatible(final Visibility visibility, final boolean isTableModel) {
    if (Objects.equals(Visibility.BOTH, visibility)) {
      return true;
    }
    return isTableModel
        ? Objects.equals(Visibility.TABLE_ONLY, visibility)
        : Objects.equals(Visibility.TREE_ONLY, visibility);
  }

  public static boolean isCompatible(final Visibility base, final Visibility incoming) {
    if (Objects.equals(base, Visibility.BOTH)) {
      return true;
    }
    if (Objects.equals(base, Visibility.TREE_ONLY)) {
      return Objects.equals(incoming, Visibility.TREE_ONLY);
    }
    if (Objects.equals(base, Visibility.TABLE_ONLY)) {
      return Objects.equals(incoming, Visibility.TABLE_ONLY);
    }
    return false;
  }

  public static boolean isCompatible(
      final Visibility pipeVisibility, final Visibility... pluginVisibilities) {
    return Arrays.stream(pluginVisibilities)
        .allMatch(pluginVisibility -> isCompatible(pluginVisibility, pipeVisibility));
  }

  public static Visibility calculateFromPluginClass(final Class<?> pipePluginClass) {
    final boolean isTreeModelAnnotationPresent =
        pipePluginClass.isAnnotationPresent(TreeModel.class);
    final boolean isTableModelAnnotationPresent =
        pipePluginClass.isAnnotationPresent(TableModel.class);
    if (!isTreeModelAnnotationPresent && !isTableModelAnnotationPresent) {
      return Visibility.TREE_ONLY; // default to tree only if missing annotations
    } else if (isTreeModelAnnotationPresent && !isTableModelAnnotationPresent) {
      return Visibility.TREE_ONLY;
    } else if (!isTreeModelAnnotationPresent && isTableModelAnnotationPresent) {
      return Visibility.TABLE_ONLY;
    } else {
      return Visibility.BOTH;
    }
  }

  public static Visibility calculateFromExtractorParameters(
      final PipeParameters extractorParameters) {
    // visible under all model when 'mode.double-living' is set to true
    final boolean isDoubleLiving =
        extractorParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE);
    if (isDoubleLiving) {
      return Visibility.BOTH;
    }

    final boolean isTreeDialect =
        extractorParameters
            .getStringOrDefault(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
            .equals(SystemConstant.SQL_DIALECT_TREE_VALUE);
    final Boolean _isCaptureTree =
        extractorParameters.getBooleanByKeys(
            PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
            PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY);
    final boolean isCaptureTree = Objects.nonNull(_isCaptureTree) ? _isCaptureTree : isTreeDialect;
    final Boolean _isCaptureTable =
        extractorParameters.getBooleanByKeys(
            PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
            PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY);
    final boolean isCaptureTable =
        Objects.nonNull(_isCaptureTable) ? _isCaptureTable : !isTreeDialect;

    // visible under specific tree or table model <-> actually capture tree or table data
    if (isCaptureTree && isCaptureTable) {
      return Visibility.BOTH;
    }
    if (isCaptureTree) {
      return Visibility.TREE_ONLY;
    }
    if (isCaptureTable) {
      return Visibility.TABLE_ONLY;
    }

    // UNREACHABLE CODE
    LOGGER.error(
        "BROKEN INVARIANT: DETECT INVISIBLE EXTRACTOR PARAMETERS {}",
        extractorParameters.getAttribute());
    return Visibility.NONE;
  }

  public static Visibility calculateFromTopicConfig(final TopicConfig config) {
    final boolean isTreeDialect =
        config
            .getStringOrDefault(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
            .equals(SystemConstant.SQL_DIALECT_TREE_VALUE);
    return !isTreeDialect ? Visibility.TABLE_ONLY : Visibility.TREE_ONLY;
  }
}
