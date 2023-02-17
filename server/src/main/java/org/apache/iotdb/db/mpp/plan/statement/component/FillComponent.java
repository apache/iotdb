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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.literal.Literal;

/** This class maintains information of {@code FILL} clause. */
public class FillComponent extends StatementNode {

  private FillPolicy fillPolicy;
  private Literal fillValue;

  public FillComponent() {}

  public FillPolicy getFillPolicy() {
    return fillPolicy;
  }

  public void setFillPolicy(FillPolicy fillPolicy) {
    this.fillPolicy = fillPolicy;
  }

  public Literal getFillValue() {
    return fillValue;
  }

  public void setFillValue(Literal fillValue) {
    this.fillValue = fillValue;
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("FILL(");
    if (fillPolicy != FillPolicy.VALUE) {
      sqlBuilder.append(fillPolicy.toString());
    } else {
      sqlBuilder.append(fillValue.toString());
    }
    sqlBuilder.append(')');
    return sqlBuilder.toString();
  }
}
