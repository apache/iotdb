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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RangeQuantifier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PatternRecognitionAnalysis {
  private final Set<String> allLabels;
  private final Set<String> undefinedLabels;
  private final Map<NodeRef<RangeQuantifier>, Analysis.Range> ranges;

  public PatternRecognitionAnalysis(
      Set<String> allLabels,
      Set<String> undefinedLabels,
      Map<NodeRef<RangeQuantifier>, Analysis.Range> ranges) {
    this.allLabels = requireNonNull(allLabels, "allLabels is null");
    this.undefinedLabels = ImmutableSet.copyOf(undefinedLabels);
    this.ranges = ImmutableMap.copyOf(ranges);
  }

  public Set<String> getAllLabels() {
    return allLabels;
  }

  public Set<String> getUndefinedLabels() {
    return undefinedLabels;
  }

  public Map<NodeRef<RangeQuantifier>, Analysis.Range> getRanges() {
    return ranges;
  }

  @Override
  public String toString() {
    return "PatternRecognitionAnalysis{"
        + "allLabels="
        + allLabels
        + ", undefinedLabels="
        + undefinedLabels
        + ", ranges="
        + ranges
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PatternRecognitionAnalysis that = (PatternRecognitionAnalysis) o;
    return Objects.equals(allLabels, that.allLabels)
        && Objects.equals(undefinedLabels, that.undefinedLabels)
        && Objects.equals(ranges, that.ranges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allLabels, undefinedLabels, ranges);
  }

  public static class PatternFunctionAnalysis {
    private final Expression expression;
    private final Descriptor descriptor;

    public PatternFunctionAnalysis(Expression expression, Descriptor descriptor) {
      this.expression = requireNonNull(expression, "expression is null");
      this.descriptor = requireNonNull(descriptor, "descriptor is null");
    }

    public Expression getExpression() {
      return expression;
    }

    public Descriptor getDescriptor() {
      return descriptor;
    }

    @Override
    public String toString() {
      return "PatternFunctionAnalysis{"
          + "expression="
          + expression
          + ", descriptor="
          + descriptor
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PatternFunctionAnalysis that = (PatternFunctionAnalysis) o;
      return Objects.equals(expression, that.expression)
          && Objects.equals(descriptor, that.descriptor);
    }

    @Override
    public int hashCode() {
      return Objects.hash(expression, descriptor);
    }
  }

  public enum NavigationMode {
    RUNNING,
    FINAL
  }

  public interface Descriptor {}

  public static class AggregationDescriptor implements Descriptor {
    private final ResolvedFunction function;
    private final List<Expression> arguments;
    private final NavigationMode mode;
    private final Set<String> labels;
    private final List<FunctionCall> matchNumberCalls;
    private final List<FunctionCall> classifierCalls;

    public AggregationDescriptor(
        ResolvedFunction function,
        List<Expression> arguments,
        NavigationMode mode,
        Set<String> labels,
        List<FunctionCall> matchNumberCalls,
        List<FunctionCall> classifierCalls) {
      this.function = requireNonNull(function, "function is null");
      this.arguments = requireNonNull(arguments, "arguments is null");
      this.mode = requireNonNull(mode, "mode is null");
      this.labels = requireNonNull(labels, "labels is null");
      this.matchNumberCalls = requireNonNull(matchNumberCalls, "matchNumberCalls is null");
      this.classifierCalls = requireNonNull(classifierCalls, "classifierCalls is null");
    }

    public ResolvedFunction getFunction() {
      return function;
    }

    public List<Expression> getArguments() {
      return arguments;
    }

    public NavigationMode getMode() {
      return mode;
    }

    public Set<String> getLabels() {
      return labels;
    }

    public List<FunctionCall> getMatchNumberCalls() {
      return matchNumberCalls;
    }

    public List<FunctionCall> getClassifierCalls() {
      return classifierCalls;
    }

    @Override
    public String toString() {
      return "AggregationDescriptor{"
          + "function="
          + function
          + ", arguments="
          + arguments
          + ", mode="
          + mode
          + ", labels="
          + labels
          + ", matchNumberCalls="
          + matchNumberCalls
          + ", classifierCalls="
          + classifierCalls
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AggregationDescriptor that = (AggregationDescriptor) o;
      return Objects.equals(function, that.function)
          && Objects.equals(arguments, that.arguments)
          && mode == that.mode
          && Objects.equals(labels, that.labels)
          && Objects.equals(matchNumberCalls, that.matchNumberCalls)
          && Objects.equals(classifierCalls, that.classifierCalls);
    }

    @Override
    public int hashCode() {
      return Objects.hash(function, arguments, mode, labels, matchNumberCalls, classifierCalls);
    }
  }

  public static class ScalarInputDescriptor implements Descriptor {
    // label indicates which column to access
    // navigation indicates which row to access.
    private final Optional<String> label;
    private final Navigation navigation;

    public ScalarInputDescriptor(Optional<String> label, Navigation navigation) {
      this.label = requireNonNull(label, "label is null");
      this.navigation = requireNonNull(navigation, "navigation is null");
    }

    public Optional<String> getLabel() {
      return label;
    }

    public Navigation getNavigation() {
      return navigation;
    }

    @Override
    public String toString() {
      return "ScalarInputDescriptor{" + "label=" + label + ", navigation=" + navigation + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ScalarInputDescriptor that = (ScalarInputDescriptor) o;
      return Objects.equals(label, that.label) && Objects.equals(navigation, that.navigation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(label, navigation);
    }
  }

  public static class ClassifierDescriptor implements Descriptor {
    private final Optional<String> label;
    private final Navigation navigation;

    public ClassifierDescriptor(Optional<String> label, Navigation navigation) {
      this.label = requireNonNull(label, "label is null");
      this.navigation = requireNonNull(navigation, "navigation is null");
    }

    public Optional<String> getLabel() {
      return label;
    }

    public Navigation getNavigation() {
      return navigation;
    }

    @Override
    public String toString() {
      return "ClassifierDescriptor{" + "label=" + label + ", navigation=" + navigation + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClassifierDescriptor that = (ClassifierDescriptor) o;
      return Objects.equals(label, that.label) && Objects.equals(navigation, that.navigation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(label, navigation);
    }
  }

  public static class MatchNumberDescriptor implements Descriptor {}

  public enum NavigationAnchor {
    FIRST,
    LAST
  }

  public static class Navigation {
    public static final Navigation DEFAULT =
        new Navigation(NavigationAnchor.LAST, NavigationMode.RUNNING, 0, 0);

    private final NavigationAnchor anchor;
    private final NavigationMode mode;
    private final int logicalOffset;
    private final int physicalOffset;

    public Navigation(
        NavigationAnchor anchor, NavigationMode mode, int logicalOffset, int physicalOffset) {
      this.anchor = requireNonNull(anchor, "anchor is null");
      this.mode = requireNonNull(mode, "mode is null");
      this.logicalOffset = logicalOffset;
      this.physicalOffset = physicalOffset;
    }

    public NavigationAnchor getAnchor() {
      return anchor;
    }

    public NavigationMode getMode() {
      return mode;
    }

    public int getLogicalOffset() {
      return logicalOffset;
    }

    public int getPhysicalOffset() {
      return physicalOffset;
    }
  }
}
