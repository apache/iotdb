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

package org.apache.iotdb.db.sql.statement.component;

import org.apache.iotdb.db.query.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class WithoutPolicy {

  WithoutPolicyType withoutPolicyType = WithoutPolicyType.NULL;

  List<Expression> withoutNullColumns = new ArrayList<>();

  public WithoutPolicyType getWithoutPolicyType() {
    return withoutPolicyType;
  }

  public void setWithoutPolicyType(WithoutPolicyType withoutPolicyType) {
    this.withoutPolicyType = withoutPolicyType;
  }

  public void addWithoutNullColumn(Expression e) {
    withoutNullColumns.add(e);
  }

  public List<Expression> getWithoutNullColumns() {
    return withoutNullColumns;
  }

  public void setWithoutNullColumns(List<Expression> withoutNullColumns) {
    this.withoutNullColumns = withoutNullColumns;
  }

  public enum WithoutPolicyType {
    NULL,
    ANY,
    ALL
  }
}
