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
package org.apache.iotdb.calcite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer;
import org.apache.iotdb.tsfile.read.common.Path;

public class IoTDBFilter extends Filter implements IoTDBRel {

  private final FilterOperator filterOperator;
  private final List<String> fieldNames;
  private List<String> predicates;  // for global predicate
  Map<String, String> deviceToFilterMap = new LinkedHashMap<>();  // for device predicate

  protected IoTDBFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition)
      throws LogicalOptimizeException {
    super(cluster, traits, child, condition);
    this.fieldNames = IoTDBRules.IoTDBFieldNames(getRowType());
    this.filterOperator = getIoTDBOperator(condition);
    this.predicates = translateWhere(filterOperator);

    // add global predicate to each device if both global and device predicate exist
    if (!this.predicates.isEmpty() && !this.deviceToFilterMap.isEmpty()) {
      for (String device : deviceToFilterMap.keySet()) {
        StringBuilder builder = new StringBuilder(this.deviceToFilterMap.get(device));
        builder.append(Util.toString(predicates, " OR ", " OR ", ""));
        this.deviceToFilterMap.put(device, builder.toString());
      }
    }
    assert getConvention() == IoTDBRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
    try {
      return new IoTDBFilter(getCluster(), traitSet, input, condition);
    } catch (LogicalOptimizeException e) {
      throw new AssertionError(e.getMessage());
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.add(deviceToFilterMap, predicates);
  }

  /**
   * Translates {@link RexNode} expressions into IoTDB filter operator.
   */
  private FilterOperator getIoTDBOperator(RexNode filter) {
    switch (filter.getKind()) {
      case EQUALS:
        return getBasicOperator(SQLConstant.EQUAL, (RexCall) filter);
      case NOT_EQUALS:
        return getBasicOperator(SQLConstant.NOTEQUAL, (RexCall) filter);
      case GREATER_THAN:
        return getBasicOperator(SQLConstant.GREATERTHAN, (RexCall) filter);
      case GREATER_THAN_OR_EQUAL:
        return getBasicOperator(SQLConstant.GREATERTHANOREQUALTO, (RexCall) filter);
      case LESS_THAN:
        return getBasicOperator(SQLConstant.LESSTHAN, (RexCall) filter);
      case LESS_THAN_OR_EQUAL:
        return getBasicOperator(SQLConstant.LESSTHANOREQUALTO, (RexCall) filter);
      case AND:
        return getBinaryOperator(SQLConstant.KW_AND, ((RexCall) filter).getOperands());
      case OR:
        return getBinaryOperator(SQLConstant.KW_OR, ((RexCall) filter).getOperands());
      default:
        return null;
    }
  }

  private FilterOperator getBasicOperator(int tokenIntType, RexCall call) {
    final RexNode left = call.operands.get(0);
    final RexNode right = call.operands.get(1);

    FilterOperator operator = getBasicOperator2(tokenIntType, left, right);
    if (operator != null) {
      return operator;
    }
    operator = getBasicOperator2(tokenIntType, right, left);
    if (operator != null) {
      return operator;
    }
    throw new AssertionError("cannnot translate basic operator: " + call);
  }

  private FilterOperator getBasicOperator2(int tokenIntType, RexNode left, RexNode right) {
    switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return null;
    }
    final RexLiteral rightLiteral = (RexLiteral) right;
    switch (left.getKind()) {
      case INPUT_REF:
        String name = fieldNames.get(((RexInputRef) left).getIndex());
        return new BasicFunctionOperator(tokenIntType, new Path(name), literalValue(rightLiteral));
      default:
        return null;
    }
  }

  private FilterOperator getBinaryOperator(int tokenIntType, List<RexNode> operands) {
    FilterOperator filterBinaryTree = new FilterOperator(tokenIntType);
    FilterOperator currentNode = filterBinaryTree;
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0 && i < operands.size() - 1) {
        FilterOperator newInnerNode = new FilterOperator(tokenIntType);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      currentNode.addChildOperator(getIoTDBOperator(operands.get(i)));
    }
    return filterBinaryTree;
  }

  /**
   * Produce the IoTDB global predicate string and devices' respective predicate for the given
   * operator.
   * <p>
   * e.g. WHERE ( device = 'd1' AND time < 10) OR (device = 'd2' AND s0 < 5) OR s1 > 100 will get
   * device list [d1,d2] and deviceToFilterMap [d1 -> time < 10, d2 -> d2.s0 < 5], and return global
   * predicate s1 > 100
   *
   * @param operator Condition to translate
   * @return IoTDB global predicate string
   */
  private List<String> translateWhere(FilterOperator operator) throws LogicalOptimizeException {
    List<String> globalPredicate = new ArrayList<>();
    if (operator.isLeaf()) {
      if (translateLeaf((BasicFunctionOperator) operator) != null) {
        globalPredicate.add(translateLeaf((BasicFunctionOperator) operator));
      }
      return globalPredicate;
    }
    DnfFilterOptimizer dnfFilterOptimizer = new DnfFilterOptimizer();
    FilterOperator dnfOperator = dnfFilterOptimizer.optimize(operator);

    if (dnfOperator.getTokenIntType() == SQLConstant.KW_AND) {
      String strAnd = translateAnd(dnfOperator);
      if (strAnd != null) {
        globalPredicate.add(strAnd);
      }
    } else {
      // get conjunction children in disjunction normal form
      List<FilterOperator> children = dnfOperator.getChildren();
      for (FilterOperator child : children) {
        String childAnd = translateAnd(child);
        if (childAnd != null) {
          globalPredicate.add(childAnd);
        }
      }
    }

    return globalPredicate;
  }

  private String translateLeaf(BasicFunctionOperator operator) {
    if (operator.getPath().equals(IoTDBConstant.DeviceColumn)) {
      // If the device doesn't exist, add it. Otherwise do nothing.
      if (!this.deviceToFilterMap.containsKey(operator.getValue())) {
        this.deviceToFilterMap.put(operator.getValue(), null);
      }
      return null;
    } else {
      // note that leaf node is a global predicate
      return translateBasicOperator(null, operator);
    }
  }

  /**
   * Translate a conjunction predicate to a IoTDB expression string.
   *
   * @param operator A conjunctive predicate
   * @return IoTDB where clause string for the predicate
   */
  private String translateAnd(FilterOperator operator) {
    if (operator.isLeaf()) {
      return translateLeaf((BasicFunctionOperator) operator);
    }
    List<String> predicates = new ArrayList<>();
    String deviceName = null;
    // e.g. Device = d1 AND Time > 15 AND s0 > 5
    List<FilterOperator> children = operator.getChildren();
    Iterator<FilterOperator> iter = children.iterator();

    // to check whether the conjunction is with device restriction
    while (iter.hasNext()) {
      FilterOperator child = iter.next();
      if (child instanceof BasicFunctionOperator
          && ((BasicFunctionOperator) child).getPath().equals(IoTDBConstant.DeviceColumn)) {

        String device = ((BasicFunctionOperator) child).getValue();
        // e.g. Device = d1 AND Device = d2
        if (deviceName != null && !deviceName.equals(device)) {
          throw new AssertionError(
              "Wrong restrictions to device: " + deviceName + " AND " + device);
        } else if (deviceName == null) {
          deviceName = device;
        }
        iter.remove();
      }
    }

    // translate the operator to string
    for (FilterOperator child : children) {
      if (child instanceof BasicFunctionOperator) {
        predicates.add(translateBasicOperator(deviceName, (BasicFunctionOperator) child));
      } else {
        throw new AssertionError("cannot translate" + child.toString());
      }
    }

    // if a conjunction has no device restriction which means it's a global restriction
    if (deviceName == null) {
      return Util.toString(predicates, "", " AND ", "");
    } else {
      if (this.deviceToFilterMap.get(deviceName) != null) {
        StringBuilder builder = new StringBuilder(this.deviceToFilterMap.get(deviceName));
        builder.append(Util.toString(predicates, " OR ", " AND ", ""));
        this.deviceToFilterMap.put(deviceName, builder.toString());
      } else {
        this.deviceToFilterMap.put(deviceName, Util.toString(predicates, "", " AND ", ""));
      }
      return null;
    }
  }

  private String translateBasicOperator(String device, BasicFunctionOperator operator) {
    StringBuilder buf = new StringBuilder();
    if (device != null && !operator.getPath().equals(IoTDBConstant.TimeColumn)) {
      buf.append(device + IoTDBConstant.PATH_SEPARATOR);
    }
    buf.append(operator.getPath());

    switch (operator.getTokenIntType()) {
      case SQLConstant.EQUAL:
        return buf.append("=").append(operator.getValue()).toString();
      case SQLConstant.NOTEQUAL:
        return buf.append("!=").append(operator.getValue()).toString();
      case SQLConstant.GREATERTHAN:
        return buf.append(">").append(operator.getValue()).toString();
      case SQLConstant.GREATERTHANOREQUALTO:
        return buf.append(">=").append(operator.getValue()).toString();
      case SQLConstant.LESSTHAN:
        return buf.append("<").append(operator.getValue()).toString();
      case SQLConstant.LESSTHANOREQUALTO:
        return buf.append("<=").append(operator.getValue()).toString();
      default:
        throw new AssertionError("cannot translate " + operator);
    }
  }

  /**
   * Convert the value of a literal to a string.
   *
   * @param literal Literal to translate
   * @return String representation of the literal
   */
  private static String literalValue(RexLiteral literal) {
    Object value = literal.getValue2();
    StringBuilder buf = new StringBuilder();
    buf.append(value);
    return buf.toString();
  }

}
