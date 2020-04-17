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

package org.apache.iotdb.cluster.query.fill;

import java.io.IOException;
import java.util.Set;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class ClusterPreviousFill extends PreviousFill {

  private MetaGroupMember metaGroupMember;
  private TimeValuePair fillResult;

  public ClusterPreviousFill(PreviousFill fill, MetaGroupMember metaGroupMember) {
    super(fill.getDataType(), fill.getQueryTime(), fill.getBeforeRange());
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void configureFill(Path path, TSDataType dataType, long queryTime, Set<String> deviceMeasurements,
      QueryContext context) throws StorageEngineException, QueryProcessException {
    super.configureFill(path, dataType, queryTime, deviceMeasurements, context);
    Filter timeFilter = constructFilter();
    fillResult = metaGroupMember.performPreviousFill(path, dataType, queryTime, getBeforeRange(),
        deviceMeasurements, context, timeFilter);
  }

  @Override
  public TimeValuePair getFillResult() {
    return fillResult;
  }
}
