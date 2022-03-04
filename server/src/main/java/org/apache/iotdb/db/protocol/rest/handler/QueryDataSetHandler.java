/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryDataSetHandler {

  private QueryDataSetHandler() {}

  public static Response fillDateSet(QueryDataSet dataSet, QueryPlan queryPlan) {
    org.apache.iotdb.db.protocol.rest.model.QueryDataSet queryDataSet =
        new org.apache.iotdb.db.protocol.rest.model.QueryDataSet();

    try {
      List<ResultColumn> resultColumns = queryPlan.getResultColumns();
      int[] dataSetIndexes = new int[resultColumns.size()];
      Map<String, Integer> sourcePathToQueryDataSetIndex = queryPlan.getPathToIndex();
      for (int i = 0; i < resultColumns.size(); i++) {
        ResultColumn resultColumn = resultColumns.get(i);
        queryDataSet.addExpressionsItem(resultColumn.getResultColumnName());
        queryDataSet.addValuesItem(new ArrayList<>());
        dataSetIndexes[i] = sourcePathToQueryDataSetIndex.get(resultColumn.getResultColumnName());
      }

      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        List<Field> fields = rowRecord.getFields();

        queryDataSet.addTimestampsItem(rowRecord.getTimestamp());
        for (int i = 0; i < resultColumns.size(); i++) {
          List<Object> values = queryDataSet.getValues().get(i);
          Field field = fields.get(dataSetIndexes[i]);
          if (field == null || field.getDataType() == null) {
            values.add(null);
          } else {
            values.add(
                field.getDataType().equals(TSDataType.TEXT)
                    ? field.getStringValue()
                    : field.getObjectValue(field.getDataType()));
          }
        }
      }
    } catch (IOException e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
    return Response.ok().entity(queryDataSet).build();
  }

  public static Response fillVariablesResult(QueryDataSet dataSet, PhysicalPlan physicalPlan)
      throws IOException {
    List<String> results = new ArrayList<>();
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      List<Field> fields = rowRecord.getFields();
      String nodePaths = fields.get(0).getObjectValue(fields.get(0).getDataType()).toString();
      if (physicalPlan instanceof ShowChildPathsPlan) {
        String[] nodeSubPath = nodePaths.split("\\.");
        results.add(nodeSubPath[nodeSubPath.length - 1]);
      } else {
        results.add(nodePaths);
      }
    }
    return Response.ok().entity(results).build();
  }
}
