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

package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class FillEngineExecutor {

  private long jobId;
  private List<Path> selectedSeries;
  private long queryTime;
  private Map<TSDataType, IFill> typeIFillMap;

  public FillEngineExecutor(long jobId, List<Path> selectedSeries, long queryTime,
      Map<TSDataType, IFill> typeIFillMap) {
    this.jobId = jobId;
    this.selectedSeries = selectedSeries;
    this.queryTime = queryTime;
    this.typeIFillMap = typeIFillMap;
  }

  /**
   * execute deserialize.
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context)
      throws StorageEngineException, PathErrorException, IOException {

    List<IFill> fillList = new ArrayList<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (Path path : selectedSeries) {
      TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
      dataTypeList.add(dataType);
      IFill fill;
      if (!typeIFillMap.containsKey(dataType)) {
        fill = new PreviousFill(dataType, queryTime, 0);
      } else {
        fill = typeIFillMap.get(dataType).copy(path);
      }
      fill.setDataType(dataType);
      fill.setQueryTime(queryTime);
      fill.constructReaders(path, context);
      fillList.add(fill);
    }

    List<IPointReader> readers = new ArrayList<>();
    for (IFill fill : fillList) {
      readers.add(fill.getFillResult());
    }

    return new EngineDataSetWithoutValueFilter(selectedSeries, dataTypeList, readers);
  }

}
