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

package org.apache.iotdb.db.queryengine.transformation.dag.column.leaf;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class TimeColumnTransformer extends LeafColumnTransformer {
  public TimeColumnTransformer(Type returnType) {
    super(returnType);
  }

  @Override
  protected void evaluate() {
    initializeColumnCache(input.getTimeColumn());
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    ColumnBuilder builder = TIMESTAMP.createColumnBuilder(selection.length);
    Column timeColumn = input.getTimeColumn();
    for (int i = 0; i < selection.length; i++) {
      if (!selection[i] || timeColumn.isNull(i)) {
        builder.appendNull();
      } else {
        builder.write(timeColumn, i);
      }
    }
    initializeColumnCache(builder.build());
  }
}
