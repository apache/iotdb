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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static org.apache.iotdb.rpc.TSStatusCode.INTERNAL_SERVER_ERROR;

public class LeadingHint extends JoinOrderHint {
  public static String hintName = "leading";
  private final List<String> tables;

  private List<String> addJoinParameters;
  private List<String> normalizedParameters;

  private final List<Pair<Set<Identifier>, Expression>> filters = new ArrayList<>();
  private final Map<String, PlanNode> relationToScanMap = new HashMap<>();

  private Set<Identifier> innerJoinTables = ImmutableSet.of();

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

  public List<Pair<Set<Identifier>, Expression>> getFilters() {
    return filters;
  }

  public Map<String, PlanNode> getRelationToScanMap() {
    return relationToScanMap;
  }

  public Set<Identifier> getInnerJoinTables() {
    return innerJoinTables;
  }

  public void setInnerJoinTables(Set<Identifier> innerJoinTables) {
    this.innerJoinTables = innerJoinTables;
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
        PlanNode logicalPlan = getPlanByName(item);
        if (logicalPlan == null) {
          return null;
        }
        logicalPlan = makeFilterPlanIfExist(getFilters(), logicalPlan);
        stack.push(logicalPlan);
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

  public PlanNode getPlanByName(String name) {
    if (!relationToScanMap.containsKey(name)) {
      return null;
    }
    return relationToScanMap.get(name);
  }

  private PlanNode makeJoinPlan(PlanNode leftChild, PlanNode rightChild) {
    List<Expression> conditions = getJoinConditions(getFilters(), leftChild, rightChild);
    JoinNode.JoinType joinType = computeJoinType();
    if (joinType == null) {
      return null;
    } else if (!isConditionJoinTypeMatched()) {
      return null;
    }

    List<Symbol> leftOutputSymbols = leftChild.getOutputSymbols();
    List<Symbol> rightOutputSymbols = rightChild.getOutputSymbols();
    Optional<JoinNode.AsofJoinClause> asofCriteria = Optional.empty();
    List<JoinNode.EquiJoinClause> criteria = new ArrayList<>();

    for (Expression conjunct : conditions) {
      ComparisonExpression equality = (ComparisonExpression) conjunct;
      Symbol leftSymbol = Symbol.from(equality.getLeft());
      Symbol rightSymbol = Symbol.from(equality.getRight());

      if (leftOutputSymbols.contains(leftSymbol) && rightOutputSymbols.contains(rightSymbol)) {
        criteria.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
      } else if (leftOutputSymbols.contains(rightSymbol)
          && rightOutputSymbols.contains(leftSymbol)) {
        criteria.add(new JoinNode.EquiJoinClause(rightSymbol, leftSymbol));
      } else {
        throw new IllegalArgumentException("Invalid join condition");
      }
    }

    return new JoinNode(
        new PlanNodeId("join"),
        joinType,
        leftChild,
        rightChild,
        criteria,
        asofCriteria,
        leftOutputSymbols,
        rightOutputSymbols,
        Optional.empty(),
        Optional.empty(),
        getTables(leftChild),
        getTables(rightChild));
  }

  private List<Expression> getJoinConditions(
      List<Pair<Set<Identifier>, Expression>> filters, PlanNode left, PlanNode right) {
    List<Expression> joinConditions = new ArrayList<>();
    for (int i = filters.size() - 1; i >= 0; i--) {
      Pair<Set<Identifier>, Expression> filterPair = filters.get(i);
      Set<Identifier> tables = Sets.union(getTables(left), getTables(right));
      // left one is smaller set
      if (tables.containsAll(filterPair.left)) {
        joinConditions.add(filterPair.right);
        filters.remove(i);
      }
    }
    return joinConditions;
  }

  private PlanNode makeFilterPlanIfExist(
      List<Pair<Set<Identifier>, Expression>> filters, PlanNode plan) {
    if (filters.isEmpty()) {
      return plan;
    }
    for (int i = filters.size() - 1; i >= 0; i--) {
      Pair<Set<Identifier>, Expression> filterPair = filters.get(i);
      if (getTables(plan).containsAll(filterPair.left)) {
        plan = new FilterNode(plan.getPlanNodeId(), plan, filterPair.right);
        filters.remove(i);
      }
    }
    return plan;
  }

  private Set<Identifier> getTables(PlanNode root) {
    if (root instanceof JoinNode) {
      return Sets.union(((JoinNode) root).getLeftTables(), ((JoinNode) root).getRightTables());
    } else if (root instanceof DeviceTableScanNode) {
      return ImmutableSet.of(
          new Identifier(((DeviceTableScanNode) root).getQualifiedObjectName().getObjectName()));
    } else if (root instanceof FilterNode) {
      return getTables(((FilterNode) root).getChild());
    } else if (root instanceof ProjectNode) {
      return getTables(((ProjectNode) root).getChild());
    } else {
      return null;
    }
  }

  public JoinNode.JoinType computeJoinType() {
    return JoinNode.JoinType.INNER;
  }

  public boolean isConditionJoinTypeMatched() {
    return true;
  }
}
