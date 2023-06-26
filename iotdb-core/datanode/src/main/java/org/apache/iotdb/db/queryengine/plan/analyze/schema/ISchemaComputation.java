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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

/**
 * This interface defines the required behaviour invoked during schemaengine fetch/computation, which is
 * executed by schemaengine fetcher.
 */
public interface ISchemaComputation {

  PartialPath getDevicePath();

  String[] getMeasurements();

  /** @param isAligned whether the fetched device is aligned */
  void computeDevice(boolean isAligned);

  /**
   * @param index the index of fetched measurement in array returned by getMeasurements
   * @param measurementSchemaInfo the measurement schemaengine of fetched measurement
   */
  void computeMeasurement(int index, IMeasurementSchemaInfo measurementSchemaInfo);

  // region used by logical view
  boolean hasLogicalViewNeedProcess();

  /**
   * @return the logical view schemaengine list recorded by this statement. It may be NULL if it is not
   *     used before.
   */
  List<LogicalViewSchema> getLogicalViewSchemaList();

  /**
   * @return the index list of logical view paths, where source of views should be placed. For
   *     example, IndexListOfLogicalViewPaths[alpha] = beta, then you should use
   *     LogicalViewSchemaList[alpha] to fill measurementSchema[beta].
   */
  List<Integer> getIndexListOfLogicalViewPaths();

  /**
   * Record the beginning and ending of logical schemaengine list. After calling this interface, the range
   * should be record. For example, the range is [0,4) which means 4 schemas exist. Later, more 3
   * schemas are added, this function is called, then it records [4,7).
   */
  void recordRangeOfLogicalViewSchemaListNow();

  /** @return the recorded range of logical view schemaengine list. */
  Pair<Integer, Integer> getRangeOfLogicalViewSchemaListRecorded();

  /**
   * @param index the index of fetched measurement in array returned by getMeasurements
   * @param measurementSchemaInfo the measurement schemaengine of source of the logical view
   * @param isAligned whether the source of this view is aligned.
   */
  void computeMeasurementOfView(
      int index, IMeasurementSchemaInfo measurementSchemaInfo, boolean isAligned);
  // endregion
}
