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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeGt;
import org.apache.iotdb.tsfile.read.filter.TimeFilter.TimeGtEq;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class LastQueryPlan extends RawDataQueryPlan {

  public LastQueryPlan() {
    super();
    setOperatorType(Operator.OperatorType.LAST);
  }

  @Override
  public void setExpression(IExpression expression) throws QueryProcessException {
    if (isValidExpression(expression)) {
      super.setExpression(expression);
    } else {
      throw new QueryProcessException("Only '>' and '>=' are supported in LAST query");
    }
  }

  // Only > and >= are supported in time filter
  private boolean isValidExpression(IExpression expression) {
    if (expression instanceof GlobalTimeExpression) {
      Filter filter = ((GlobalTimeExpression) expression).getFilter();
      if (filter instanceof TimeGtEq || filter instanceof TimeGt) {
        return true;
      }
    }
    return false;
  }
}
