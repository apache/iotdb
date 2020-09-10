/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.reader.series;

import java.io.IOException;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;

public class DescSeriesReaderByTimestamp extends SeriesReaderByTimestamp {

  public DescSeriesReaderByTimestamp(PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      TsFileFilter fileFilter) {
    seriesReader = new SeriesReader(seriesPath, allSensors, dataType, context,
        dataSource, TimeFilter.ltEq(Long.MAX_VALUE),
        null, fileFilter, false);
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    seriesReader.setTimeFilter(timestamp);
    if ((batchData == null || (batchData.getTimeByIndex(0) > timestamp))
        && !hasNext(timestamp)) {
      return null;
    }

    return batchData.getValueInTimestamp(timestamp, (c, p) -> c > p);
  }
}
