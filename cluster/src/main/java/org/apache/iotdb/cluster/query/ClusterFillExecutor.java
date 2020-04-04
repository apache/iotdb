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
package org.apache.iotdb.cluster.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.FillQueryExecutor;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class ClusterFillExecutor extends FillQueryExecutor {

  private MetaGroupMember metaGroupMember;

  public ClusterFillExecutor(List<Path> selectedSeries,
      List<TSDataType> dataTypes,
      long queryTime,
      Map<TSDataType, IFill> typeIFillMap,
      MetaGroupMember metaGroupMember) {
    super(selectedSeries, dataTypes, queryTime, typeIFillMap);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected void configureFill(IFill fill, TSDataType dataType, Path path, Set<String> deviceMeasurements,
      QueryContext context,
      long queryTime) throws StorageEngineException {
    fill.setDataType(dataType);
    fill.setQueryTime(queryTime);
    fill.setAllDataReader(metaGroupMember.getSeriesReader(path, deviceMeasurements, dataType,
        fill.getFilter(), null,
     context));
  }
}
