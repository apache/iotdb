/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.utils.hint;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static org.apache.iotdb.rpc.TSStatusCode.INTERNAL_SERVER_ERROR;

public class LeadingHint extends JoinOrderHint {
  public static String hintName = "leading";
  private final List<String> tables;

  private List<String> addJoinParameters;
  private List<String> normalizedParameters;

  public LeadingHint(List<String> parameters) {
    super(hintName);
    // /* leading(t3 {}) 会报错
    this.tables = new ArrayList<>();
    addJoinParameters = insertJoinIntoParameters(parameters);
    normalizedParameters = parseIntoReversePolishNotation(addJoinParameters);

    if (tables.isEmpty()) {
      throw new IllegalArgumentException("LeaderHint accepts one or more tables");
    }
    if (hasDuplicateTable(tables)) {
      throw new IllegalArgumentException("LeaderHint accepts no duplicate tables");
    }
  }

  @Override
  public String getKey() {
    return category;
  }

  public List<String> getTables() {
    return tables;
  }

  public PlanNode generateLeadingJoinPlan() {
    Stack<PlanNode> stack = new Stack<>();
    for (String item : normalizedParameters) {
      if (item.equals("join")) {
        PlanNode rightChild = stack.pop();
        PlanNode leftChild = stack.pop();
        PlanNode joinPlan = makeJoinPlan(leftChild, rightChild);
        if (joinPlan == null) {
          return null;
        }
        stack.push(joinPlan);
      } else {
        //        PlanNode logicalPlan = getLogicalPlanByName(item);
        //        ogicalPlan = makeFilterPlanIfExist(getFilters(), logicalPlan);
        //        stack.push(logicalPlan);
      }
    }

    PlanNode finalJoin = stack.pop();
    // we want all filters been removed
    //    if (Utils.enableAssert && !filters.isEmpty()) {
    //      throw new IllegalStateException(
    //              "Leading hint process failed: filter should be empty, but meet: " + filters
    //      );
    //    }
    if (finalJoin == null) {
      throw new IoTDBRuntimeException(
          "final join plan should not be null", INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return finalJoin;
  }

  @Override
  public String toString() {
    if (tables == null || tables.isEmpty()) {
      return hintName;
    }
    return hintName + "-" + String.join("-", tables);
  }

  private boolean hasDuplicateTable(List<String> tables) {
    Set<String> tableSet = Sets.newHashSet();
    for (String table : tables) {
      if (!tableSet.add(table)) {
        return true;
      }
    }
    return false;
  }

  public static List<String> insertJoinIntoParameters(List<String> list) {
    List<String> output = new ArrayList<>();

    for (String item : list) {
      if (item.equals("{")) {
        output.add(item);
        continue;
      } else if (item.equals("}")) {
        output.remove(output.size() - 1);
        output.add(item);
      } else {
        output.add(item);
      }
      output.add("join");
    }
    output.remove(output.size() - 1);
    return output;
  }

  public List<String> parseIntoReversePolishNotation(List<String> list) {
    Stack<String> s1 = new Stack<>();
    List<String> s2 = new ArrayList<>();

    for (String item : list) {
      if (!(item.equals("{") || item.equals("}") || item.equals("join"))) {
        tables.add(item);
        s2.add(item);
      } else if (item.equals("{")) {
        s1.push(item);
      } else if (item.equals("}")) {
        while (!s1.peek().equals("{")) {
          String pop = s1.pop();
          s2.add(pop);
        }
        s1.pop();
      } else {
        while (!s1.isEmpty() && !s1.peek().equals("{")) {
          s2.add(s1.pop());
        }
        s1.push(item);
      }
    }
    while (!s1.isEmpty()) {
      s2.add(s1.pop());
    }
    return s2;
  }

  private PlanNode makeJoinPlan(PlanNode leftChild, PlanNode rightChild) {
    //    List<Expression> conditions = getJoinConditions(
    //            getFilters(), leftChild, rightChild);
    //    Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
    //            leftChild.getOutput(), rightChild.getOutput(), conditions);
    //    // leading hint would set status inside if not success
    //    JoinType joinType = computeJoinType(getBitmap(leftChild),
    //            getBitmap(rightChild), conditions);
    //    if (joinType == null) {
    //      this.setStatus(HintStatus.SYNTAX_ERROR);
    //      this.setErrorMessage("JoinType can not be null");
    //    } else if (!isConditionJoinTypeMatched(conditions, joinType)) {
    //      this.setStatus(HintStatus.UNUSED);
    //      this.setErrorMessage("condition does not matched joinType");
    //    }
    //    if (!this.isSuccess()) {
    //      return null;
    //    }
    // get joinType
    //    LogicalJoin logicalJoin = new LogicalJoin<>(joinType, pair.first,
    //            pair.second,
    //            distributeHint,
    //            Optional.empty(),
    //            leftChild,
    //            rightChild, null);
    //    logicalJoin.getJoinReorderContext().setLeadingJoin(true);
    //    logicalJoin.setBitmap(LongBitmap.or(getBitmap(leftChild), getBitmap(rightChild)));

    PlanNode joinNode = planJoin(leftChild, rightChild);
    return joinNode;
  }

  private PlanNode planJoin(PlanNode leftPlan, PlanNode rightPlan) {
    List<Symbol> leftOutputSymbols = leftPlan.getOutputSymbols();
    List<Symbol> rightOutputSymbols = rightPlan.getOutputSymbols();
    List<JoinNode.EquiJoinClause> criteria = new ArrayList<>();
    Optional<JoinNode.AsofJoinClause> asofCriteria = Optional.empty();

    return new JoinNode(
        new PlanNodeId("join"),
        JoinNode.JoinType.INNER,
        leftPlan,
        rightPlan,
        criteria,
        asofCriteria,
        leftOutputSymbols,
        rightOutputSymbols,
        Optional.empty(),
        Optional.empty());
  }
}
