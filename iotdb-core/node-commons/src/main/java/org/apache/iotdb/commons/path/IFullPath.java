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

package org.apache.iotdb.commons.path;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Accountable;

public interface IFullPath extends Accountable {

  IDeviceID getDeviceId();

  TSDataType getSeriesType();

  // don't call this method, it will be removed soon
  @Deprecated
  static IFullPath convertToIFullPath(PartialPath fullPath) {
    if (fullPath instanceof MeasurementPath) {
      return new NonAlignedFullPath(
          fullPath.getIDeviceID(), ((MeasurementPath) fullPath).getMeasurementSchema());
    } else if (fullPath instanceof AlignedPath) {
      return new AlignedFullPath(
          fullPath.getIDeviceID(),
          ((AlignedPath) fullPath).getMeasurementList(),
          ((AlignedPath) fullPath).getSchemaList());
    } else {
      throw new IllegalArgumentException("Only accept MeasurementPath and AlignedPath.");
    }
  }
}
