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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FunctionExpressionNode extends ExpressionNodeImpl {
  private String rowName;

  private String executorName;

  private TSDataType tsDataType;

  private List<ExpressionNode> subNodes;

  public FunctionExpressionNode(String nodeName, TSDataType tsDataType) {
    this.nodeName = nodeName;
    this.tsDataType = tsDataType;
  }

  public FunctionExpressionNode(
      String nodeName, String executorName, String rowName, TSDataType tsDataType) {
    this.nodeName = nodeName;
    this.executorName = executorName;
    this.tsDataType = tsDataType;
    this.rowName = rowName;
  }

  public void setRowName(String rowName) {
    this.rowName = rowName;
  }

  public void setExecutorName(String executorName) {
    this.executorName = executorName;
  }

  public String getType() {
    switch (tsDataType) {
      case INT32:
        return "int";
      case TEXT:
        return "long";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case BOOLEAN:
        return "boolean";
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported for udtf codegen.", tsDataType));
    }
  }

  @Override
  public String toCode() {
    //    return "(" + getType() + ")" + "UDTFCaller.udtfCall(" + executorName + ", " + rowName +
    // ")";
    return "UDTFCaller.udtfCall(" + executorName + ", " + rowName + ").toString()";
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    if (subNodes.size() == 0) {
      // TODO: should throw a exception
      return null;
    }
    if (subNodes.size() == 1) {
      return new IsNullExpressionNode(subNodes.get(0), true);
    }
    ExpressionNode retNode = subNodes.get(0);
    for (int i = 1; i < subNodes.size(); ++i) {
      retNode =
          new BinaryExpressionNode(
              "||",
              new IsNullExpressionNode(retNode, true),
              new IsNullExpressionNode(subNodes.get(i), true));
    }
    return retNode;
  }

  @Override
  public String toExpendCode() {
    return toCode();
  }

  public void addSubExpressionNode(ExpressionNode subNode) {
    if (Objects.isNull(subNodes)) {
      subNodes = new ArrayList<>();
    }
    subNodes.add(subNode);
  }
}
