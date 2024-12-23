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

package org.apache.iotdb.db.queryengine.common.schematree;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Set;

public interface ISchemaTree {
  /**
   * Return all measurement paths for given path pattern and filter the result by slimit and offset.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return Left: all measurement paths; Right: remaining series offset
   */
  @TestOnly
  Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(
      PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch);

  /**
   * Return all measurement paths for given path pattern.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @return Left: all measurement paths; Right: remaining series offset
   */
  Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(PartialPath pathPattern);

  /**
   * Get all device matching the path pattern.
   *
   * @param pathPattern the pattern of the target devices.
   * @return A HashSet instance which stores info of the devices matching the given path pattern.
   */
  List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern);

  DeviceSchemaInfo searchDeviceSchemaInfo(PartialPath devicePath, List<String> measurements);

  /**
   * Get database name by device path
   *
   * <p>e.g., root.sg1 is a database and device path = root.sg1.d1, return root.sg1
   *
   * @param pathName only full device path, cannot be path pattern
   * @return database in the given path
   */
  String getBelongedDatabase(IDeviceID pathName);

  String getBelongedDatabase(PartialPath path);

  Set<String> getDatabases();

  void setDatabases(Set<String> databases);

  boolean isEmpty();

  void mergeSchemaTree(ISchemaTree schemaTree);

  /**
   * Check whether this schema tree has normal series(not template series).
   *
   * @return true if it has normal series, else false
   */
  boolean hasNormalTimeSeries();

  /**
   * Get all templates being used in this schema tree.
   *
   * @return template list
   */
  List<Template> getUsingTemplates();

  /**
   * Get all devices using the given template.
   *
   * @param templateId template id
   * @return device path list
   */
  List<PartialPath> getDeviceUsingTemplate(int templateId);

  /**
   * If there is view in this schema tree, return true, else return false.
   *
   * @return whether there's view in this schema tree
   */
  boolean hasLogicalViewMeasurement();

  void removeLogicalView();
}
