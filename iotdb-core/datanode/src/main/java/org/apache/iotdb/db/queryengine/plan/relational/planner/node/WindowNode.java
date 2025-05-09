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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame;
import org.apache.iotdb.db.queryengine.plan.relational.utils.DataOrganizationSpecification;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FrameBound.Type.CURRENT_ROW;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FrameBound.Type.UNBOUNDED_PRECEDING;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame.Type.RANGE;

public class WindowNode extends SingleChildProcessNode {
  private final Set<Symbol> prePartitionedInputs;
  private final DataOrganizationSpecification specification;
  private final int preSortedOrderPrefix;
  private final Map<Symbol, Function> windowFunctions;
  private final Optional<Symbol> hashSymbol;

  public WindowNode(
      PlanNodeId id,
      PlanNode child,
      DataOrganizationSpecification specification,
      Map<Symbol, Function> windowFunctions,
      Optional<Symbol> hashSymbol,
      Set<Symbol> prePartitionedInputs,
      int preSortedOrderPrefix) {
    super(id, child);
    // Make the defensive copy eagerly, so it can be used for both the validation checks and
    // assigned directly to the field afterwards
    prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);

    ImmutableSet<Symbol> partitionBy = ImmutableSet.copyOf(specification.getPartitionBy());
    Optional<OrderingScheme> orderingScheme = specification.getOrderingScheme();
    checkArgument(
        partitionBy.containsAll(prePartitionedInputs),
        "prePartitionedInputs must be contained in partitionBy");
    checkArgument(
        preSortedOrderPrefix == 0
            || (orderingScheme.isPresent()
                && preSortedOrderPrefix <= orderingScheme.get().getOrderBy().size()),
        "Cannot have sorted more symbols than those requested");
    checkArgument(
        preSortedOrderPrefix == 0 || partitionBy.equals(prePartitionedInputs),
        "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

    this.prePartitionedInputs = prePartitionedInputs;
    this.specification = specification;
    this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
    this.hashSymbol = hashSymbol;
    this.preSortedOrderPrefix = preSortedOrderPrefix;
  }

  @Override
  public PlanNode clone() {
    return new WindowNode(
        id,
        child,
        specification,
        windowFunctions,
        hashSymbol,
        prePartitionedInputs,
        preSortedOrderPrefix);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.copyOf(concat(child.getOutputSymbols(), windowFunctions.keySet()));
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new WindowNode(
        id,
        Iterables.getOnlyElement(newChildren),
        specification,
        windowFunctions,
        hashSymbol,
        prePartitionedInputs,
        preSortedOrderPrefix);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitWindowFunction(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  public Set<Symbol> getPrePartitionedInputs() {
    return prePartitionedInputs;
  }

  public DataOrganizationSpecification getSpecification() {
    return specification;
  }

  public int getPreSortedOrderPrefix() {
    return preSortedOrderPrefix;
  }

  public Map<Symbol, Function> getWindowFunctions() {
    return windowFunctions;
  }

  public Optional<Symbol> getHashSymbol() {
    return hashSymbol;
  }

  @Immutable
  public static class Frame {
    public static final Frame DEFAULT_FRAME =
        new WindowNode.Frame(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final WindowFrame.Type type;
    private final FrameBound.Type startType;
    private final Optional<Symbol> startValue;
    private final Optional<Symbol> sortKeyCoercedForFrameStartComparison;
    private final FrameBound.Type endType;
    private final Optional<Symbol> endValue;
    private final Optional<Symbol> sortKeyCoercedForFrameEndComparison;

    // This information is only used for printing the plan.
    private final Optional<Expression> originalStartValue;
    private final Optional<Expression> originalEndValue;

    public Frame(
        WindowFrame.Type type,
        FrameBound.Type startType,
        Optional<Symbol> startValue,
        Optional<Symbol> sortKeyCoercedForFrameStartComparison,
        FrameBound.Type endType,
        Optional<Symbol> endValue,
        Optional<Symbol> sortKeyCoercedForFrameEndComparison,
        Optional<Expression> originalStartValue,
        Optional<Expression> originalEndValue) {
      this.startType = requireNonNull(startType, "startType is null");
      this.startValue = requireNonNull(startValue, "startValue is null");
      this.sortKeyCoercedForFrameStartComparison =
          requireNonNull(
              sortKeyCoercedForFrameStartComparison,
              "sortKeyCoercedForFrameStartComparison is null");
      this.endType = requireNonNull(endType, "endType is null");
      this.endValue = requireNonNull(endValue, "endValue is null");
      this.sortKeyCoercedForFrameEndComparison =
          requireNonNull(
              sortKeyCoercedForFrameEndComparison, "sortKeyCoercedForFrameEndComparison is null");
      this.type = requireNonNull(type, "type is null");
      this.originalStartValue = requireNonNull(originalStartValue, "originalStartValue is null");
      this.originalEndValue = requireNonNull(originalEndValue, "originalEndValue is null");

      if (startValue.isPresent()) {
        checkArgument(
            originalStartValue.isPresent(),
            "originalStartValue must be present if startValue is present");
        if (type == RANGE) {
          checkArgument(
              sortKeyCoercedForFrameStartComparison.isPresent(),
              "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if startValue is present");
        }
      }

      if (endValue.isPresent()) {
        checkArgument(
            originalEndValue.isPresent(),
            "originalEndValue must be present if endValue is present");
        if (type == RANGE) {
          checkArgument(
              sortKeyCoercedForFrameEndComparison.isPresent(),
              "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if endValue is present");
        }
      }
    }

    public WindowFrame.Type getType() {
      return type;
    }

    public FrameBound.Type getStartType() {
      return startType;
    }

    public Optional<Symbol> getStartValue() {
      return startValue;
    }

    public Optional<Symbol> getSortKeyCoercedForFrameStartComparison() {
      return sortKeyCoercedForFrameStartComparison;
    }

    public FrameBound.Type getEndType() {
      return endType;
    }

    public Optional<Symbol> getEndValue() {
      return endValue;
    }

    public Optional<Symbol> getSortKeyCoercedForFrameEndComparison() {
      return sortKeyCoercedForFrameEndComparison;
    }

    public Optional<Expression> getOriginalStartValue() {
      return originalStartValue;
    }

    public Optional<Expression> getOriginalEndValue() {
      return originalEndValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Frame frame = (Frame) o;
      return type == frame.type
          && startType == frame.startType
          && Objects.equals(startValue, frame.startValue)
          && Objects.equals(
              sortKeyCoercedForFrameStartComparison, frame.sortKeyCoercedForFrameStartComparison)
          && endType == frame.endType
          && Objects.equals(endValue, frame.endValue)
          && Objects.equals(
              sortKeyCoercedForFrameEndComparison, frame.sortKeyCoercedForFrameEndComparison);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          type,
          startType,
          startValue,
          sortKeyCoercedForFrameStartComparison,
          endType,
          endValue,
          sortKeyCoercedForFrameEndComparison);
    }
  }

  @Immutable
  public static final class Function {
    private final ResolvedFunction resolvedFunction;
    private final List<Expression> arguments;
    private final Frame frame;
    private final boolean ignoreNulls;

    public Function(
        ResolvedFunction resolvedFunction,
        List<Expression> arguments,
        Frame frame,
        boolean ignoreNulls) {
      this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
      this.arguments = requireNonNull(arguments, "arguments is null");
      this.frame = requireNonNull(frame, "frame is null");
      this.ignoreNulls = ignoreNulls;
    }

    public ResolvedFunction getResolvedFunction() {
      return resolvedFunction;
    }

    public Frame getFrame() {
      return frame;
    }

    public List<Expression> getArguments() {
      return arguments;
    }

    @Override
    public int hashCode() {
      return Objects.hash(resolvedFunction, arguments, frame, ignoreNulls);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      Function other = (Function) obj;
      return Objects.equals(this.resolvedFunction, other.resolvedFunction)
          && Objects.equals(this.arguments, other.arguments)
          && Objects.equals(this.frame, other.frame)
          && this.ignoreNulls == other.ignoreNulls;
    }

    public boolean isIgnoreNulls() {
      return ignoreNulls;
    }
  }
}
