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

package org.apache.iotdb.db.protocol.influxdb.statement;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

public class InfluxQueryStatement extends QueryStatement {

  private InfluxSelectComponent influxSelectComponent;

  public InfluxQueryStatement() {
    super();
  }

  @Override
  public InfluxSelectComponent getSelectComponent() {
    return influxSelectComponent;
  }

  @Override
  public void setSelectComponent(SelectComponent influxSelectComponent) {
    this.influxSelectComponent = (InfluxSelectComponent) influxSelectComponent;
  }

  @Override
  public void semanticCheck() {
    if (influxSelectComponent.isHasMoreSelectorFunction()
        && influxSelectComponent.isHasCommonQuery()) {
      throw new SemanticException(
          "ERR: mixing multiple selector functions with tags or fields is not supported");
    }
    if (influxSelectComponent.isHasAggregationFunction()
        && influxSelectComponent.isHasCommonQuery()) {
      throw new SemanticException(
          "ERR: mixing aggregate and non-aggregate queries is not supported");
    }
  }
}
