/**
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
package org.apache.iotdb.cluster.query.coordinatornode.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.query.coordinatornode.manager.IClusterSingleQueryManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithTimeGenerator;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterExecutorWithTimeGenerator {

  private QueryExpression queryExpression;
  private IClusterSingleQueryManager queryManager;

  ClusterExecutorWithTimeGenerator(QueryExpression queryExpression, IClusterSingleQueryManager queryManager) {
    this.queryExpression = queryExpression;
    this.queryManager = queryManager;
  }

  /**
   * execute query.
   *
   * @return QueryDataSet object
   * @throws IOException IOException
   * @throws FileNodeManagerException FileNodeManagerException
   */
  public QueryDataSet execute(QueryContext context) throws FileNodeManagerException {

    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), queryExpression.getSelectedSeries());
    QueryResourceManager.getInstance()
        .beginQueryOfGivenExpression(context.getJobId(), queryExpression.getExpression());

    EngineTimeGenerator timestampGenerator;
    List<EngineReaderByTimeStamp> readersOfSelectedSeries;
    try {
      timestampGenerator = new EngineTimeGenerator(queryExpression.getExpression(), context);
      readersOfSelectedSeries = SeriesReaderFactory
          .getByTimestampReadersOfSelectedPaths(queryExpression.getSelectedSeries(), context);
    } catch (IOException ex) {
      throw new FileNodeManagerException(ex);
    }

    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : queryExpression.getSelectedSeries()) {
      try {
        dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
      } catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }

    }
    return new EngineDataSetWithTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
        timestampGenerator,
        readersOfSelectedSeries);
  }
}
