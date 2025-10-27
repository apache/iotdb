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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

public class ExpressionAndValuePointers {
  public static final ExpressionAndValuePointers TRUE =
      new ExpressionAndValuePointers(TRUE_LITERAL, ImmutableList.of());

  // there may be multiple functions in an expression in the MEASURES or DEFINE clause
  private final Expression expression;
  // `Assignment` indicates which symbol a function pointer has been assigned to
  private final List<Assignment> assignments;

  public ExpressionAndValuePointers(Expression expression, List<Assignment> assignments) {
    this.expression = requireNonNull(expression, "expression is null");
    this.assignments = ImmutableList.copyOf(assignments);
  }

  public Expression getExpression() {
    return expression;
  }

  public List<Assignment> getAssignments() {
    return assignments;
  }

  public List<Symbol> getInputSymbols() {
    Set<Symbol> localInputs =
        assignments.stream()
            .filter(
                assignment ->
                    assignment.getValuePointer() instanceof ClassifierValuePointer
                        || assignment.getValuePointer() instanceof MatchNumberValuePointer)
            .map(Assignment::getSymbol)
            .collect(toImmutableSet());

    ImmutableList.Builder<Symbol> inputSymbols = ImmutableList.builder();
    for (Assignment assignment : assignments) {
      ValuePointer valuePointer = assignment.getValuePointer();

      if (valuePointer instanceof ScalarValuePointer) {
        ScalarValuePointer pointer = (ScalarValuePointer) valuePointer;
        Symbol symbol = pointer.getInputSymbol();
        if (!localInputs.contains(symbol)) {
          inputSymbols.add(symbol);
        }
      }
    }

    return inputSymbols.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ExpressionAndValuePointers o = (ExpressionAndValuePointers) obj;
    return Objects.equals(expression, o.expression) && Objects.equals(assignments, o.assignments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, assignments);
  }

  public static void serialize(ExpressionAndValuePointers valuePointers, ByteBuffer byteBuffer) {
    Expression.serialize(valuePointers.expression, byteBuffer);
    ReadWriteIOUtils.write(valuePointers.assignments.size(), byteBuffer);
    for (Assignment assignment : valuePointers.assignments) {
      Assignment.serialize(assignment, byteBuffer);
    }
  }

  public static void serialize(ExpressionAndValuePointers valuePointers, DataOutputStream stream)
      throws IOException {
    Expression.serialize(valuePointers.expression, stream);
    ReadWriteIOUtils.write(valuePointers.assignments.size(), stream);
    for (Assignment assignment : valuePointers.assignments) {
      Assignment.serialize(assignment, stream);
    }
  }

  public static ExpressionAndValuePointers deserialize(ByteBuffer byteBuffer) {
    Expression expression = Expression.deserialize(byteBuffer);
    int assignmentsSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Assignment> assignments = new ArrayList<>(assignmentsSize);
    for (int i = 0; i < assignmentsSize; i++) {
      assignments.add(Assignment.deserialize(byteBuffer));
    }
    return new ExpressionAndValuePointers(expression, assignments);
  }

  public static class Assignment {
    private final Symbol symbol;
    private final ValuePointer valuePointer;

    public Assignment(Symbol symbol, ValuePointer valuePointer) {
      this.symbol = symbol;
      this.valuePointer = valuePointer;
    }

    public Symbol getSymbol() {
      return symbol;
    }

    public ValuePointer getValuePointer() {
      return valuePointer;
    }

    @Override
    public int hashCode() {
      return Objects.hash(symbol, valuePointer);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Assignment that = (Assignment) o;
      return Objects.equals(symbol, that.symbol) && Objects.equals(valuePointer, that.valuePointer);
    }

    @Override
    public String toString() {
      return "Assignment{" + "symbol=" + symbol + ", valuePointer=" + valuePointer + '}';
    }

    public static void serialize(Assignment assignment, ByteBuffer byteBuffer) {
      Symbol.serialize(assignment.symbol, byteBuffer);

      if (assignment.valuePointer instanceof MatchNumberValuePointer) {
        ReadWriteIOUtils.write(0, byteBuffer);
      } else if (assignment.valuePointer instanceof ClassifierValuePointer) {
        ReadWriteIOUtils.write(1, byteBuffer);
      } else if (assignment.valuePointer instanceof ScalarValuePointer) {
        ReadWriteIOUtils.write(2, byteBuffer);
      } else if (assignment.valuePointer instanceof AggregationValuePointer) {
        ReadWriteIOUtils.write(3, byteBuffer);
      } else {
        throw new IllegalArgumentException("Unknown ValuePointer type");
      }

      if (assignment.valuePointer instanceof MatchNumberValuePointer) {
        MatchNumberValuePointer.serialize(
            (MatchNumberValuePointer) assignment.valuePointer, byteBuffer);
      } else if (assignment.valuePointer instanceof ClassifierValuePointer) {
        ClassifierValuePointer.serialize(
            (ClassifierValuePointer) assignment.valuePointer, byteBuffer);
      } else if (assignment.valuePointer instanceof ScalarValuePointer) {
        ScalarValuePointer.serialize((ScalarValuePointer) assignment.valuePointer, byteBuffer);
      } else if (assignment.valuePointer instanceof AggregationValuePointer) {
        AggregationValuePointer.serialize(
            (AggregationValuePointer) assignment.valuePointer, byteBuffer);
      } else {
        throw new IllegalArgumentException("Unknown ValuePointer type");
      }
    }

    public static void serialize(Assignment assignment, DataOutputStream stream)
        throws IOException {
      Symbol.serialize(assignment.symbol, stream);

      if (assignment.valuePointer instanceof MatchNumberValuePointer) {
        ReadWriteIOUtils.write(0, stream);
      } else if (assignment.valuePointer instanceof ClassifierValuePointer) {
        ReadWriteIOUtils.write(1, stream);
      } else if (assignment.valuePointer instanceof ScalarValuePointer) {
        ReadWriteIOUtils.write(2, stream);
      } else if (assignment.valuePointer instanceof AggregationValuePointer) {
        ReadWriteIOUtils.write(3, stream);
      } else {
        throw new IllegalArgumentException("Unknown ValuePointer type");
      }

      if (assignment.valuePointer instanceof MatchNumberValuePointer) {
        MatchNumberValuePointer.serialize(
            (MatchNumberValuePointer) assignment.valuePointer, stream);
      } else if (assignment.valuePointer instanceof ClassifierValuePointer) {
        ClassifierValuePointer.serialize((ClassifierValuePointer) assignment.valuePointer, stream);
      } else if (assignment.valuePointer instanceof ScalarValuePointer) {
        ScalarValuePointer.serialize((ScalarValuePointer) assignment.valuePointer, stream);
      } else if (assignment.valuePointer instanceof AggregationValuePointer) {
        AggregationValuePointer.serialize(
            (AggregationValuePointer) assignment.valuePointer, stream);
      } else {
        throw new IllegalArgumentException("Unknown ValuePointer type");
      }
    }

    public static Assignment deserialize(ByteBuffer byteBuffer) {
      Symbol symbol = Symbol.deserialize(byteBuffer);

      int type = ReadWriteIOUtils.readInt(byteBuffer);
      ValuePointer valuePointer;

      if (type == 0) {
        valuePointer = MatchNumberValuePointer.deserialize(byteBuffer);
      } else if (type == 1) {
        valuePointer = ClassifierValuePointer.deserialize(byteBuffer);
      } else if (type == 2) {
        valuePointer = ScalarValuePointer.deserialize(byteBuffer);
      } else if (type == 3) {
        valuePointer = AggregationValuePointer.deserialize(byteBuffer);
      } else {
        throw new IllegalArgumentException("Unknown ValuePointer type");
      }

      return new Assignment(symbol, valuePointer);
    }
  }
}
