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
package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;

/**
 * A timestamp generator for query with filter. e.g. For query clause "select s1, s2 from root where
 * s3 < 0 and time > 100", this class can iterate back to every timestamp of the query.
 */
public class ServerTimeGenerator extends TimeGenerator {

  protected QueryContext context;
  protected RawDataQueryPlan queryPlan;

  public ServerTimeGenerator(QueryContext context) {
    this.context = context;
  }

  /**
   * Constructor of EngineTimeGenerator.
   */
  public ServerTimeGenerator(IExpression expression, QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    this.context = context;
    this.queryPlan = queryPlan;
    try {
      super.constructNode(expression);
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    Filter filter = expression.getFilter();
    Path path = expression.getSeriesPath();
    TSDataType dataType;
    QueryDataSource queryDataSource;
    try {
      dataType = IoTDB.metaManager.getSeriesType(path.getFullPath());
      queryDataSource = QueryResourceManager.getInstance().getQueryDataSource(path, context, filter);
      // update filter by TTL
      filter = queryDataSource.updateFilterUsingTTL(filter);
    } catch (Exception e) {
      throw new IOException(e);
    }

    return new SeriesRawDataBatchReader(path, queryPlan.getAllMeasurementsInDevice(path.getDevice()), dataType, context, queryDataSource, null, filter, null);
  }
}
