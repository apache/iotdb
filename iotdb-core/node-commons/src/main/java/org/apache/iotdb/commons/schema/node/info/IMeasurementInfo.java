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
package org.apache.iotdb.commons.schema.node.info;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

public interface IMeasurementInfo {

  IMeasurementSchema getSchema();

  void setSchema(IMeasurementSchema schema);

  TSDataType getDataType();

  String getAlias();

  void setAlias(String alias);

  long getOffset();

  void setOffset(long offset);

  boolean isPreDeleted();

  void setPreDeleted(boolean preDeleted);

  int estimateSize();

  void moveDataToNewMNode(IMeasurementMNode<?> newMNode);

  // Alias series properties methods
  /**
   * Check if this measurement is renamed (is an alias series).
   *
   * @return true if this is an alias series, false otherwise
   */
  boolean isRenamed();

  /**
   * Set the IS_RENAMED flag for this measurement.
   *
   * @param isRenamed true if this is an alias series, false otherwise
   */
  void setIsRenamed(boolean isRenamed);

  /**
   * Check if this measurement is currently being renamed (for non-procedure alter operations).
   *
   * @return true if this measurement is being renamed, false otherwise
   */
  boolean isRenaming();

  /**
   * Set the IS_RENAMING flag for this measurement.
   *
   * @param isRenaming true if this measurement is being renamed, false otherwise
   */
  void setIsRenaming(boolean isRenaming);

  /**
   * Check if this measurement is disabled (original physical path after renaming).
   *
   * @return true if this measurement is disabled, false otherwise
   */
  boolean isDisabled();

  /**
   * Set the DISABLED flag for this measurement.
   *
   * @param isDisabled true if this measurement is disabled, false otherwise
   */
  void setDisabled(boolean isDisabled);

  /**
   * Get the original physical path if this is an alias series.
   *
   * @return the original physical path, or null if not an alias series
   */
  PartialPath getOriginalPath();

  /**
   * Set the original physical path for this alias series.
   *
   * @param originalPath the original physical path
   */
  void setOriginalPath(PartialPath originalPath);

  /**
   * Get the alias path pointing to this physical series.
   *
   * @return the alias path, or null if not set
   */
  PartialPath getAliasPath();

  /**
   * Set the alias path pointing to this physical series.
   *
   * @param aliasPath the alias path
   */
  void setAliasPath(PartialPath aliasPath);
}
