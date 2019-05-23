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
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * IoTDB query executor of Stand-alone version with global time filter.
 */
public class EngineExecutorWithoutTimeGenerator extends AbstractExecutorWithoutTimeGenerator {

  private QueryExpression queryExpression;

  public EngineExecutorWithoutTimeGenerator(QueryExpression queryExpression) {
    this.queryExpression = queryExpression;
  }

  /**
   * without filter or with global time filter.
   */
  public QueryDataSet execute(QueryContext context)
      throws FileNodeManagerException {

    Filter timeFilter = null;
    if (queryExpression.hasQueryFilter()) {
      timeFilter = ((GlobalTimeExpression) queryExpression.getExpression()).getFilter();
    }

    List<IPointReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), queryExpression.getSelectedSeries());

    for (Path path : queryExpression.getSelectedSeries()) {

      IPointReader reader = createSeriesReader(context, path, dataTypes, timeFilter);
      readersOfSelectedSeries.add(reader);
    }

    try {
      return new EngineDataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
          readersOfSelectedSeries);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }
  }
}
