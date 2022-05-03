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

package org.apache.iotdb.db.query.expression.unary;

import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ExpressionType;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.db.query.udf.core.transformer.unary.InTransformer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashSet;

public class InExpression extends UnaryExpression {

  private final boolean isNotIn;
  private final LinkedHashSet<String> values;

  public InExpression(Expression expression, boolean isNotIn, LinkedHashSet<String> values) {
    super(expression);
    this.isNotIn = isNotIn;
    this.values = values;
  }

  public InExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    isNotIn = ReadWriteIOUtils.readBool(byteBuffer);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    values = new LinkedHashSet<>();
    for (int i = 0; i < size; ++i) {
      values.add(ReadWriteIOUtils.readString(byteBuffer));
    }
  }

  public boolean isNotIn() {
    return isNotIn;
  }

  public LinkedHashSet<String> getValues() {
    return values;
  }

  @Override
  protected String getExpressionStringInternal() {
    StringBuilder valuesStringBuilder = new StringBuilder();
    Iterator<String> iterator = values.iterator();
    if (iterator.hasNext()) {
      valuesStringBuilder.append(iterator.next());
    }
    while (iterator.hasNext()) {
      valuesStringBuilder.append(", ").append(iterator.next());
    }
    return expression + " IN (" + valuesStringBuilder + ")";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.IN;
  }

  @Override
  protected Transformer constructTransformer(LayerPointReader pointReader) {
    return new InTransformer(pointReader, isNotIn, values);
  }

  @Override
  protected Expression constructExpression(Expression childExpression) {
    return new InExpression(childExpression, isNotIn, values);
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNotIn, byteBuffer);
    ReadWriteIOUtils.write(values.size(), byteBuffer);
    for (String value : values) {
      ReadWriteIOUtils.write(value, byteBuffer);
    }
  }
}
