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

package org.apache.iotdb.db.queryengine.transformation.dag.column.ternary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.read.common.type.Type;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;

public abstract class CompareTernaryColumnTransformer extends TernaryColumnTransformer {
  protected CompareTernaryColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
  }

  @Override
  protected final void checkType() {
    if (firstColumnTransformer.isReturnTypeNumeric()
            && secondColumnTransformer.isReturnTypeNumeric()
            && thirdColumnTransformer.isReturnTypeNumeric()
        || isCharType(firstColumnTransformer.getType())
            && isCharType(secondColumnTransformer.getType())
            && isCharType(thirdColumnTransformer.getType())) {
      return;
    }

    throw new UnsupportedOperationException(
        "The Type of three subExpression should be all Numeric or Text");
  }
}
