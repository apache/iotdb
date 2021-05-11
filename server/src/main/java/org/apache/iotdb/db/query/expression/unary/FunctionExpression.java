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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FunctionExpression implements Expression {

  private final String functionName;
  private final Map<String, String> functionAttributes;

  private List<Expression> expressions;
  private List<TSDataType> dataTypes;

  private String expressionString;
  private String parametersString;

  public FunctionExpression(String functionName) {
    this.functionName = functionName;
    functionAttributes = new LinkedHashMap<>();
    expressions = new ArrayList<>();
  }

  public FunctionExpression(String functionName, Map<String, String> functionAttributes,
      List<Expression> expressions) {
    this.functionName = functionName;
    this.functionAttributes = functionAttributes;
    this.expressions = expressions;
  }

  public void addAttribute(String key, String value) {
    functionAttributes.put(key, value);
  }

  public void addExpression(Expression expression) {
    expressions.add(expression);
  }

  public void setExpressions(List<Expression> expressions) {
    this.expressions = expressions;
  }

  public String getFunctionName() {
    return functionName;
  }

  public Map<String, String> getFunctionAttributes() {
    return functionAttributes;
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  @Override
  public TSDataType dataType() {
    // TODO: the expression type is determined in runtime
    throw new NotImplementedException();
  }

  public List<TSDataType> getDataTypes() throws MetadataException {
    if (dataTypes == null) {
      dataTypes = new ArrayList<>();
      for (Expression expression : expressions) {
        dataTypes.add(expression.dataType());
      }
    }
    return dataTypes;
  }

  @Override
  public String toString() {
    if (expressionString == null) {
      expressionString = functionName + "(" + parametersString() + ")";
    }
    return expressionString;
  }

  /**
   * Generates the parameter part of the udf column name.
   *
   * <p>Example:
   * Full column name -> udf(root.sg.d.s1, sin(root.sg.d.s1), 'key1'='value1', 'key2'='value2')
   * <p>
   * The parameter part -> root.sg.d.s1, sin(root.sg.d.s1), 'key1'='value1', 'key2'='value2'
   */
  private String parametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder();
      if (!expressions.isEmpty()) {
        builder.append(expressions.get(0).toString());
        for (int i = 1; i < expressions.size(); ++i) {
          builder.append(", ").append(expressions.get(i).toString());
        }
      }
      if (!functionAttributes.isEmpty()) {
        if (!expressions.isEmpty()) {
          builder.append(", ");
        }
        Iterator<Entry<String, String>> iterator = functionAttributes.entrySet().iterator();
        Entry<String, String> entry = iterator.next();
        builder
            .append("\"")
            .append(entry.getKey())
            .append("\"=\"")
            .append(entry.getValue())
            .append("\"");
        while (iterator.hasNext()) {
          entry = iterator.next();
          builder
              .append(", ")
              .append("\"")
              .append(entry.getKey())
              .append("\"=\"")
              .append(entry.getValue())
              .append("\"");
        }
      }
      parametersString = builder.toString();
    }
    return parametersString;
  }
}
