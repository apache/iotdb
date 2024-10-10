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
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;

public class ConstantColumnTransformer extends LeafColumnTransformer {

  private final Column value;

  public ConstantColumnTransformer(Type returnType, Column value) {
    super(returnType);
    this.value = value;
  }

  @Override
  protected void evaluate() {
    initializeColumnCache(new RunLengthEncodedColumn(value, input.getPositionCount()));
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    int positionCount = input.getPositionCount();
    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    for (int i = 0; i < positionCount; i++) {
      if (!selection[i] || value.isNull(i)) {
        builder.appendNull();
      } else {
        builder.write(value, 0);
      }
    }
    initializeColumnCache(builder.build());
  }
}
