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

import java.util.Arrays;
import java.util.Objects;

public class VisibilityUtils {

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
    if (isStrictVisibility(extractorParameters)) {
      return calculateStrictlyFromExtractorParameters(extractorParameters);
    }

    if (PipeSourceConstant.isDoubleLiving(extractorParameters)) {
      return Visibility.BOTH;
    }

    return calculateFromCaptureParameters(extractorParameters, isTreeDialect(extractorParameters));
  }

  private static Visibility calculateFromCaptureParameters(
      final PipeParameters extractorParameters, final boolean isTreeDialect) {
    final boolean isCaptureTree =
        Objects.requireNonNullElse(
            extractorParameters.getBooleanByKeys(
                PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
                PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY),
            isTreeDialect);
    final boolean isCaptureTable =
        Objects.requireNonNullElse(
            extractorParameters.getBooleanByKeys(
                PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
                PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY),
            !isTreeDialect);

    if (isCaptureTree && isCaptureTable) {
      return Visibility.BOTH;
    }
    if (isCaptureTree) {
      return Visibility.TREE_ONLY;
    }
    if (isCaptureTable) {
      return Visibility.TABLE_ONLY;
    }
    return Visibility.NONE;
  }

  public static boolean isTreeModelDataAllowToBeCaptured(final PipeParameters sourceParameters) {
    final Visibility visibility = calculateCaptureFromExtractorParameters(sourceParameters);
    return Objects.equals(Visibility.BOTH, visibility)
        || Objects.equals(Visibility.TREE_ONLY, visibility);
  }

  public static boolean isTableModelDataAllowToBeCaptured(final PipeParameters sourceParameters) {
    final Visibility visibility = calculateCaptureFromExtractorParameters(sourceParameters);
    return Objects.equals(Visibility.BOTH, visibility)
        || Objects.equals(Visibility.TABLE_ONLY, visibility);
  }

  public static boolean isStrictVisibility(final PipeParameters sourceParameters) {
    return SystemConstant.PIPE_VISIBILITY_STRICT_VALUE.equals(
        sourceParameters.getStringOrDefault(SystemConstant.PIPE_VISIBILITY_KEY, ""));
  }

  private static Visibility calculateStrictlyFromExtractorParameters(
      final PipeParameters extractorParameters) {
    return isTreeDialect(extractorParameters) ? Visibility.TREE_ONLY : Visibility.TABLE_ONLY;
  }

  private static Visibility calculateCaptureFromExtractorParameters(
      final PipeParameters extractorParameters) {
    if (isStrictVisibility(extractorParameters)) {
      if (hasCaptureAttributes(extractorParameters)) {
        return calculateFromCaptureParameters(
            extractorParameters, isTreeDialect(extractorParameters));
      }
      return calculateStrictlyFromExtractorParameters(extractorParameters);
    }

    if (PipeSourceConstant.isDoubleLiving(extractorParameters)) {
      return Visibility.BOTH;
    }
    return calculateFromCaptureParameters(extractorParameters, isTreeDialect(extractorParameters));
  }

  private static boolean hasCaptureAttributes(final PipeParameters extractorParameters) {
    return extractorParameters.hasAnyAttributes(
        PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
        PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY,
        PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
        PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY);
  }

  private static boolean isTreeDialect(final PipeParameters extractorParameters) {
    return extractorParameters
        .getStringOrDefault(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
        .equals(SystemConstant.SQL_DIALECT_TREE_VALUE);
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
