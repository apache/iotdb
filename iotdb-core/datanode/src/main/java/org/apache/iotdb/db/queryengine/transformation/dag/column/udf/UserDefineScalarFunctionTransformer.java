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

package org.apache.iotdb.db.queryengine.transformation.dag.column.udf;

import org.apache.iotdb.commons.udf.access.RecordIterator;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.MultiColumnTransformer;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.stream.Collectors;

public class UserDefineScalarFunctionTransformer extends MultiColumnTransformer {

  private final ScalarFunction scalarFunction;
  private final List<Type> inputTypes;

  public UserDefineScalarFunctionTransformer(
      Type returnType,
      ScalarFunction scalarFunction,
      List<ColumnTransformer> childrenTransformers) {
    super(returnType, childrenTransformers);
    this.scalarFunction = scalarFunction;
    this.inputTypes =
        childrenTransformers.stream().map(ColumnTransformer::getType).collect(Collectors.toList());
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    RecordIterator iterator = new RecordIterator(childrenColumns, inputTypes, positionCount);
    while (iterator.hasNext()) {
      try {
        Object result = scalarFunction.evaluate(iterator.next());
        if (result == null) {
          builder.appendNull();
        } else {
          builder.writeObject(result);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Error occurs when evaluating user-defined scalar function "
                + scalarFunction.getClass().getName(),
            e);
      }
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    RecordIterator iterator = new RecordIterator(childrenColumns, inputTypes, positionCount);
    int i = 0;
    while (iterator.hasNext()) {
      try {
        Record input = iterator.next();
        if (selection[i++]) {
          builder.appendNull();
          continue;
        }
        Object result = scalarFunction.evaluate(input);
        if (result == null) {
          builder.appendNull();
        } else {
          builder.writeObject(result);
        }
      } catch (Throwable e) {
        throw new RuntimeException(
            "Error occurs when evaluating user-defined scalar function "
                + scalarFunction.getClass().getName(),
            e);
      }
    }
  }

  @Override
  public void close() {
    super.close();
    scalarFunction.beforeDestroy();
  }

  @Override
  protected void checkType() {
    // do nothing
  }
}
