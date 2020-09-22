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

package org.apache.iotdb.db.query.dataset;

import static org.apache.iotdb.db.utils.QueryUtils.transferShowTimeSeriesResultToRecordList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowTimeseriesDataSet extends QueryDataSet {

  private static final Logger logger = LoggerFactory.getLogger(ShowTimeseriesDataSet.class);


  private final ShowTimeSeriesPlan plan;
  private List<RowRecord> result = new ArrayList<>();
  private int index = 0;
  private QueryContext context;

  public boolean hasLimit = true;

  public ShowTimeseriesDataSet(List<PartialPath> paths, List<TSDataType> dataTypes,
      ShowTimeSeriesPlan showTimeSeriesPlan, QueryContext context) {
    super(new ArrayList<>(paths), dataTypes);
    this.plan = showTimeSeriesPlan;
    this.context = context;
  }

  @Override
  protected boolean hasNextWithoutConstraint() throws IOException {
    if (index == result.size() && !hasLimit) {
      plan.setOffset(plan.getOffset() + plan.getLimit());
      try {
        List<ShowTimeSeriesResult> showTimeSeriesResults = MManager.getInstance()
            .showTimeseries(plan, context);
        result = transferShowTimeSeriesResultToRecordList(showTimeSeriesResults);
        index = 0;
      } catch (MetadataException e) {
        logger.error("Something wrong happened while showing {}", paths.stream().map(
            Path::getFullPath).reduce((a, b) -> a + "," + b), e);
        throw new IOException(e.getCause());
      }
    }
    return index < result.size();
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    return result.get(index++);
  }

  public void putRecord(RowRecord newRecord) {
    result.add(newRecord);
  }
}
