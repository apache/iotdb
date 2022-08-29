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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.service.StaticResps;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;

import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LastQueryPlan extends RawDataQueryPlan {

  public LastQueryPlan() {
    super();
    setOperatorType(Operator.OperatorType.LAST);
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException {
    List<ResultColumn> deduplicatedResultColumns = new ArrayList<>();
    Set<String> columnForReaderSet = new HashSet<>();
    for (int i = 0; i < resultColumns.size(); i++) {
      String column = resultColumns.get(i).getResultColumnName();
      if (!columnForReaderSet.contains(column)) {
        addDeduplicatedPaths(paths.get(i));
        deduplicatedResultColumns.add(resultColumns.get(i));
        columnForReaderSet.add(column);
      }
    }
    setResultColumns(deduplicatedResultColumns);
  }

  @Override
  public TSExecuteStatementResp getTSExecuteStatementResp(boolean isJdbcQuery) {
    return StaticResps.LAST_RESP.deepCopy();
  }

  @Override
  public List<TSDataType> getWideQueryHeaders(
      List<String> respColumns, List<String> respSgColumns, boolean isJdbcQuery, BitSet aliasList)
      throws TException {
    throw new TException("unsupported query type: " + getOperatorType());
  }

  @Override
  public void setExpression(IExpression expression) throws QueryProcessException {
    if (isValidExpression(expression)) {
      super.setExpression(expression);
    } else {
      throw new QueryProcessException("Only time filters are supported in LAST query");
    }
  }

  private boolean isValidExpression(IExpression expression) {
    return expression instanceof GlobalTimeExpression;
  }
}
