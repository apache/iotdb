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

package org.apache.iotdb.db.query.reader.resourceRelated;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

class ResourceRelatedUtil {

  /**
   * Returns true if the start and end time of the series data in this unsequence TsFile satisfy the
   * filter condition. Returns false if not satisfy. <p> This method is used to in the constructor
   * function to check whether this TsFile can be skipped.
   *
   * Note: Please make sure the TsFileResource contains seriesPath!
   *
   * @param tsFile the TsFileResource corresponding to this TsFile
   * @param filter filter condition. Null if no filter.
   * @return True if the TsFile's start and end time satisfy the filter condition; False if not
   * satisfy.
   */
  static boolean isTsFileSatisfied(TsFileResource tsFile, Filter filter, Path seriesPath) {
    if (filter == null) {
      return true;
    }
    long startTime = tsFile.getStartTimeMap().get(seriesPath.getDevice());
    long endTime = tsFile.getEndTimeMap().get(seriesPath.getDevice());
    return filter.satisfyStartEndTime(startTime, endTime);
  }
}
