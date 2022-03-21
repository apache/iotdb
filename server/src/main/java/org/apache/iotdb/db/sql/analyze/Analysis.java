/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.sql.analyze;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.sql.statement.Statement;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.*;

/** Analysis used for planning a query. TODO: This class may need to store more info for a query. */
public class Analysis {
  // Description for each series. Such as dataType, existence

  // Data distribution info for each series. Series -> [VSG, VSG]

  // Map<PartialPath, List<FullPath>> Used to remove asterisk

  // Statement
  private Statement statement;

  // DataPartitionInfo
  private Map<String, Map<DataRegionTimeSlice, List<DataRegion>>> dataPartitionInfo;

  // SchemaPartitionInfo
  private Map<String, List<SchemaRegion>> schemaPartitionInfo;

  public Set<DataRegion> getPartitionInfo(PartialPath seriesPath, Filter timefilter) {
    if (timefilter == null) {
      // TODO: (xingtanzjr) we need to have a method to get the deviceGroup by device
      String deviceGroup = seriesPath.getDevice();
      Set<DataRegion> result = new HashSet<>();
      this.dataPartitionInfo.get(deviceGroup).values().forEach(result::addAll);
      return result;
    } else {
      // TODO: (xingtanzjr) complete this branch
      return null;
    }
  }

  public void setDataPartitionInfo(
      Map<String, Map<DataRegionTimeSlice, List<DataRegion>>> dataPartitionInfo) {
    this.dataPartitionInfo = dataPartitionInfo;
  }

  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement statement) {
    this.statement = statement;
  }
}
