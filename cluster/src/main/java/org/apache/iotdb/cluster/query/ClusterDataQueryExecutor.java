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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.exception.EmptyIntervalException;
import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.RawDataQueryExecutor;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDataQueryExecutor extends RawDataQueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterDataQueryExecutor.class);
  private MetaGroupMember metaGroupMember;
  private ClusterReaderFactory readerFactory;

  ClusterDataQueryExecutor(RawDataQueryPlan plan, MetaGroupMember metaGroupMember) {
    super(plan);
    this.metaGroupMember = metaGroupMember;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
  }

  @Override
  protected List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context)
      throws StorageEngineException {
    Filter timeFilter = null;
    if (queryPlan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
      PartialPath path = queryPlan.getDeduplicatedPaths().get(i);
      TSDataType dataType = queryPlan.getDeduplicatedDataTypes().get(i);

      ManagedSeriesReader reader;
      try {
        reader = readerFactory.getSeriesReader(path,
            queryPlan.getAllMeasurementsInDevice(path.getDevice()), dataType, timeFilter,
            null, context, queryPlan.isAscending());
      } catch (EmptyIntervalException e) {
        logger.info(e.getMessage());
        return Collections.emptyList();
      }

      readersOfSelectedSeries.add(reader);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Initialized {} readers for {}", readersOfSelectedSeries.size(), queryPlan);
    }
    return readersOfSelectedSeries;
  }

  @Override
  protected IReaderByTimestamp getReaderByTimestamp(PartialPath path,
      Set<String> deviceMeasurements, TSDataType dataType,
      QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return readerFactory.getReaderByTimestamp(path, deviceMeasurements, dataType, context,
        queryPlan.isAscending());
  }

  @Override
  protected TimeGenerator getTimeGenerator(IExpression queryExpression,
      QueryContext context, RawDataQueryPlan rawDataQueryPlan) throws StorageEngineException {
    return new ClusterTimeGenerator(queryExpression, context, metaGroupMember, rawDataQueryPlan);
  }
}
