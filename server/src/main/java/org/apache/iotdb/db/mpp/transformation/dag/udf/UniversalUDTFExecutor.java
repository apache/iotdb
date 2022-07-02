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

package org.apache.iotdb.db.mpp.transformation.dag.udf;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class UniversalUDTFExecutor extends UDTFExecutor {

  protected ElasticSerializableTVList collector;

  public UniversalUDTFExecutor(String functionName, ZoneId zoneId, UDTF udtf) {
    super(functionName, zoneId);
    this.udtf = udtf;
  }

  @Override
  public void beforeStart(
      long queryId,
      float collectorMemoryBudgetInMB,
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    super.beforeStart(
        queryId, collectorMemoryBudgetInMB, childExpressions, childExpressionDataTypes, attributes);
    collector =
        ElasticSerializableTVList.newElasticSerializableTVList(
            UDFDataTypeTransformer.transformToTsDataType(configurations.getOutputDataType()),
            queryId,
            collectorMemoryBudgetInMB,
            1);
  }

  @Override
  public void terminate() {
    try {
      udtf.terminate(collector);
    } catch (Exception e) {
      onError("terminate(PointCollector)", e);
    }
  }

  public void execute(Row row, boolean isCurrentRowNull) {
    try {
      if (isCurrentRowNull) {
        // A null row will never trigger any UDF computing
        collector.putNull(row.getTime());
      } else {
        udtf.transform(row, collector);
      }
    } catch (Exception e) {
      onError("transform(Row, PointCollector)", e);
    }
  }

  public void execute(RowWindow rowWindow) {
    try {
      udtf.transform(rowWindow, collector);
    } catch (Exception e) {
      onError("transform(RowWindow, PointCollector)", e);
    }
  }

  @Override
  public ElasticSerializableTVList getCollector() {
    return collector;
  }
}
