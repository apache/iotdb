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

package org.apache.iotdb.db.query.udf.core.input;

import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class InputLayerWithValueFilter extends InputLayer {

  public InputLayerWithValueFilter(long queryId, float memoryBudgetInMB, List<Path> paths,
      List<TSDataType> dataTypes, TimeGenerator timeGenerator, List<IReaderByTimestamp> readers,
      List<Boolean> cached) throws QueryProcessException {
    super(queryId, memoryBudgetInMB,
        new RawQueryDataSetWithValueFilter(paths, dataTypes, timeGenerator, readers, cached));
  }
}
