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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class NewSetStatement implements Statement {

  private List<String> values;

  private String varName;

  private TSDataType tsDataType;

  public NewSetStatement(String varName, List<String> values, TSDataType tsDataType) {
    this.values = values;
    this.varName = varName;
    this.tsDataType = tsDataType;
  }

  private String templateString() {
    switch (tsDataType) {
      case INT32:
        return "Integer";
      case INT64:
        return "Long";
      case FLOAT:
        return "Float";
      case DOUBLE:
        return "Double";
      case BOOLEAN:
        return "Boolean";
      case TEXT:
        return "String";
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported", tsDataType));
    }
  }

  @Override
  public String toCode() {
    StringBuilder newSetCode = new StringBuilder();
    String template = templateString();
    newSetCode
        .append("HashSet<")
        .append(template)
        .append(">")
        .append(varName)
        .append(" = ")
        .append("new HashSet<>();\n");
    for (String value : values) {
      if (tsDataType == TSDataType.TEXT) {
        newSetCode.append(varName).append(".add(\"").append(value).append("\");\n");
      } else {
        newSetCode
            .append(varName)
            .append(".add(")
            .append(template)
            .append(".valueOf(")
            .append(value)
            .append("));\n");
      }
    }
    return newSetCode.toString();
  }
}
