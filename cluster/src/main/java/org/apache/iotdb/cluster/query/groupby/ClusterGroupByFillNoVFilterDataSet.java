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
package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillWithoutValueFilterDataSet;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.Set;

/** the cluster implementation of GroupByFillWithoutValueFilterDataSet */
public class ClusterGroupByFillNoVFilterDataSet extends GroupByFillWithoutValueFilterDataSet {

  private MetaGroupMember metaGroupMember;

  public ClusterGroupByFillNoVFilterDataSet(
      QueryContext context,
      GroupByTimeFillPlan groupByTimeFillPlan,
      MetaGroupMember metaGroupMember)
      throws QueryProcessException, StorageEngineException {
    super(context, groupByTimeFillPlan);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected GroupByExecutor getGroupByExecutor(
      PartialPath path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    return new MergeGroupByExecutor(
        path, deviceMeasurements, dataType, context, timeFilter, metaGroupMember, ascending);
  }
}
