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

import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillWithValueFilterDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class ClusterGroupByFillVFilterDataSet extends GroupByFillWithValueFilterDataSet {

  private final MetaGroupMember metaGroupMember;
  private final ClusterReaderFactory readerFactory;

  public ClusterGroupByFillVFilterDataSet(
      QueryContext context,
      GroupByTimeFillPlan groupByTimeFillPlan,
      MetaGroupMember metaGroupMember) {
    super(context, groupByTimeFillPlan);
    this.metaGroupMember = metaGroupMember;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(context, metaGroupMember, rawDataQueryPlan, false);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan dataQueryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return readerFactory.getReaderByTimestamp(
        path,
        dataQueryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        dataQueryPlan.isAscending(),
        null);
  }
}
