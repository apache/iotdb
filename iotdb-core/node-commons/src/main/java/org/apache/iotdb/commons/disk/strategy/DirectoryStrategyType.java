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
package org.apache.iotdb.commons.disk.strategy;

import org.apache.iotdb.commons.i18n.UtilMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum DirectoryStrategyType {
  SEQUENCE_STRATEGY,
  MAX_DISK_USABLE_SPACE_FIRST_STRATEGY,
  MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY,
  RANDOM_ON_DISK_USABLE_SPACE_STRATEGY;

  private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryStrategyType.class);

  /**
   * Resolves the strategy type from a multi-dir strategy class name as configured by {@code
   * dn_multi_dir_strategy}. Accepts either a simple class name (e.g. {@code SequenceStrategy}) or a
   * fully-qualified one. Returns {@link #SEQUENCE_STRATEGY} for a null or unrecognized value, which
   * matches the configured default.
   */
  public static DirectoryStrategyType fromClassName(String className) {
    if (className != null) {
      String simpleName = className.substring(className.lastIndexOf('.') + 1);
      if (simpleName.equals(MaxDiskUsableSpaceFirstStrategy.class.getSimpleName())) {
        return MAX_DISK_USABLE_SPACE_FIRST_STRATEGY;
      } else if (simpleName.equals(MinFolderOccupiedSpaceFirstStrategy.class.getSimpleName())) {
        return MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY;
      } else if (simpleName.equals(RandomOnDiskUsableSpaceStrategy.class.getSimpleName())) {
        return RANDOM_ON_DISK_USABLE_SPACE_STRATEGY;
      } else if (simpleName.equals(SequenceStrategy.class.getSimpleName())) {
        return SEQUENCE_STRATEGY;
      }
      LOGGER.warn(UtilMessages.UNRECOGNIZED_MULTI_DIR_STRATEGY, className, SEQUENCE_STRATEGY);
    }
    return SEQUENCE_STRATEGY;
  }
}
