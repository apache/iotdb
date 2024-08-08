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

package org.apache.iotdb.commons.path.fa.nfa;

import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.IPatternFA;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * Given path pattern root.sg.*.s, the SimpleNFA is:
 *
 * <p>initial -(root)-> state[0] -(sg)-> state[1] -(*)-> state[2] -(s)-> state[3] <br>
 * state[3] is final state
 *
 * <p>Given path pattern root.**.s, the SimpleNFA is:
 *
 * <p>initial -(root)-> state[0] -(*)-> state[1] -(s)-> state[2] <br>
 * with extra: state[1] -(*)-> state[1] and state[2] -(*)-> state[1] <br>
 * state[3] is final state
 *
 * <p>Given path pattern root.sg.d with prefix match, the SimpleNFA is:
 *
 * <p>initial -(root)-> state[0] -(sg)-> state[1] -(d)-> state[2] -(*)-> state[3] <br>
 * with extra: state[3] -(*)-> state[3] <br>
 * both state[2] and state[3] are final states
 */
public class SimpleNFA implements IPatternFA {

  private final boolean isPrefixMatch;

  // raw nodes of pathPattern
  private final PartialPath pathPattern;
  private int smallestNullableIndex;

  // initial state of this NFA and the only transition from this state is "root"
  private final SinglePathPatternNode initialState = new InitialNode();
  // all states corresponding to raw pattern nodes, with an extra prefixMatch state
  private final SinglePathPatternNode[] patternNodes;

  public SimpleNFA(PartialPath pathPattern, boolean isPrefixMatch) {
    this.isPrefixMatch = isPrefixMatch;
    this.pathPattern = pathPattern;
    computeSmallestNullableIndex();
    patternNodes = new SinglePathPatternNode[pathPattern.getNodeLength() + 1];
  }

  // This is only used for table device query
  // For other schema query, the smallest nullable index will be the last index of the pattern
  private void computeSmallestNullableIndex() {
    smallestNullableIndex = pathPattern.getNodeLength() - 1;
    if (!(pathPattern instanceof ExtendedPartialPath)) {
      return;
    }

    while (smallestNullableIndex >= 0) {
      final String node = pathPattern.getNodes()[smallestNullableIndex];
      if (!Objects.isNull(node)
          && !(Objects.equals(node, ONE_LEVEL_PATH_WILDCARD)
              && ((ExtendedPartialPath) pathPattern).match(smallestNullableIndex, null))) {
        break;
      }
      smallestNullableIndex--;
    }
  }

  @Override
  public Map<String, IFATransition> getPreciseMatchTransition(IFAState state) {
    return getNextNode((SinglePathPatternNode) state).getPreNodePreciseMatchTransition();
  }

  @Override
  public Iterator<IFATransition> getPreciseMatchTransitionIterator(IFAState state) {
    return getNextNode((SinglePathPatternNode) state).getPreNodePreciseMatchTransitionIterator();
  }

  @Override
  public Iterator<IFATransition> getFuzzyMatchTransitionIterator(IFAState state) {
    return getNextNode((SinglePathPatternNode) state).getPreNodeFuzzyMatchTransitionIterator();
  }

  @Override
  public int getFuzzyMatchTransitionSize(IFAState state) {
    return getNextNode((SinglePathPatternNode) state).getPreNodeFuzzyMatchTransitionSize();
  }

  @Override
  public IFAState getNextState(IFAState sourceState, IFATransition transition) {
    return (SinglePathPatternNode) transition;
  }

  @Override
  public IFAState getInitialState() {
    return initialState;
  }

  @Override
  public int getStateSize() {
    return patternNodes.length;
  }

  @Override
  public IFAState getState(int index) {
    return patternNodes[index];
  }

  @Override
  public boolean mayTransitionOverlap() {
    return true;
  }

  private SinglePathPatternNode getNextNode(final SinglePathPatternNode currentNode) {
    if (currentNode.patternIndex == pathPattern.getNodeLength()) {
      return currentNode;
    }
    final String[] rawNodes = pathPattern.getNodes();
    int nextIndex = currentNode.getIndex() + 1;
    if (patternNodes[nextIndex] == null) {
      if (nextIndex == rawNodes.length) {
        patternNodes[nextIndex] = new PrefixMatchNode(nextIndex, currentNode.getTracebackNode());
      } else if (rawNodes[nextIndex] == null) {
        patternNodes[nextIndex] = new NameMatchNode(nextIndex, currentNode.getTracebackNode());
      } else if (rawNodes[nextIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        patternNodes[nextIndex] = new MultiLevelWildcardMatchNode(nextIndex);
      } else if (rawNodes[nextIndex].equals(ONE_LEVEL_PATH_WILDCARD)) {
        patternNodes[nextIndex] =
            new OneLevelWildcardMatchNode(
                nextIndex,
                currentNode.getTracebackNode(),
                pathPattern instanceof ExtendedPartialPath
                    ? event -> ((ExtendedPartialPath) pathPattern).match(nextIndex, event)
                    : event -> true);
      } else if (PathPatternUtil.hasWildcard(rawNodes[nextIndex])) {
        patternNodes[nextIndex] = new RegexMatchNode(nextIndex, currentNode.getTracebackNode());
      } else {
        patternNodes[nextIndex] = new NameMatchNode(nextIndex, currentNode.getTracebackNode());
      }
    }
    return patternNodes[nextIndex];
  }

  // Each node in raw nodes of path pattern maps to a PatternNode, which can represent a state.
  // Since the transition is defined by the node of next state, we directly let this class implement
  // IFATransition.
  private abstract class SinglePathPatternNode implements IFAState, IFATransition {

    protected final int patternIndex;

    protected final SinglePathPatternNode tracebackNode;

    private SinglePathPatternNode(int patternIndex, SinglePathPatternNode tracebackNode) {
      this.patternIndex = patternIndex;
      this.tracebackNode = tracebackNode;
    }

    @Override
    public boolean isInitial() {
      return patternIndex == -1;
    }

    @Override
    public boolean isFinal() {
      return patternIndex >= smallestNullableIndex;
    }

    @Override
    public int getIndex() {
      return patternIndex;
    }

    @Override
    public String getAcceptEvent() {
      return pathPattern.getNodes()[patternIndex];
    }

    public SinglePathPatternNode getTracebackNode() {
      return tracebackNode;
    }

    /**
     * Since the transition generation of one patternNode need to judge based on next patternNode.
     * Therefore, we implemented this method for previous node.
     *
     * @return the precise transitions of patternNode[patternIndex - 1]
     */
    protected abstract Map<String, IFATransition> getPreNodePreciseMatchTransition();

    /**
     * Since the transition generation of one patternNode need to judge based on next patternNode.
     * Therefore, we implemented this method for previous node.
     *
     * @return the precise transitions of patternNode[patternIndex - 1]
     */
    protected abstract Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator();

    /**
     * Since the transition generation of one patternNode need to judge based on next patternNode.
     * Therefore, we implemented this method for previous node.
     *
     * @return the fuzzy transitions of patternNode[patternIndex - 1]
     */
    protected abstract Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator();

    /**
     * Since the transition generation of one patternNode need to judge based on next patternNode.
     * Therefore, we implemented this method for previous node.
     *
     * @return the num of fuzzy transitions of patternNode[patternIndex - 1]
     */
    protected abstract int getPreNodeFuzzyMatchTransitionSize();

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SinglePathPatternNode patternNode = (SinglePathPatternNode) o;
      return patternIndex == patternNode.patternIndex;
    }

    @Override
    public int hashCode() {
      return patternIndex;
    }
  }

  /** Initial node with patternIndex == -1. Holds the initial transition -(root)-> */
  private class InitialNode extends SinglePathPatternNode {

    private InitialNode() {
      super(-1, null);
    }

    @Override
    public boolean isInitial() {
      return true;
    }

    @Override
    protected Map<String, IFATransition> getPreNodePreciseMatchTransition() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMatch(String event) {
      return false;
    }

    @Override
    protected Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int getPreNodeFuzzyMatchTransitionSize() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * The last node represents the prefix match state, and provides the transition access for
   * patternNodes[rawNodes.length - 1]
   */
  private class PrefixMatchNode extends SinglePathPatternNode {

    private PrefixMatchNode(int patternIndex, SinglePathPatternNode tracebackNode) {
      super(patternIndex, tracebackNode);
    }

    @Override
    public boolean isMatch(String event) {
      return true;
    }

    @Override
    protected Map<String, IFATransition> getPreNodePreciseMatchTransition() {
      return Collections.emptyMap();
    }

    @Override
    protected Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator() {
      return Collections.emptyIterator();
    }

    @Override
    protected Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator() {
      if (isPrefixMatch) {
        return new SingletonIterator<>(this);
      } else {
        if (tracebackNode == null) {
          return Collections.emptyIterator();
        } else {
          return new SingletonIterator<>(tracebackNode);
        }
      }
    }

    @Override
    protected int getPreNodeFuzzyMatchTransitionSize() {
      return isPrefixMatch || tracebackNode != null ? 1 : 0;
    }
  }

  /** The patternNode of the rawNode **. */
  private class MultiLevelWildcardMatchNode extends SinglePathPatternNode {

    private MultiLevelWildcardMatchNode(int patternIndex) {
      super(patternIndex, null);
    }

    @Override
    public SinglePathPatternNode getTracebackNode() {
      return this;
    }

    @Override
    public boolean isMatch(String event) {
      return true;
    }

    @Override
    protected Map<String, IFATransition> getPreNodePreciseMatchTransition() {
      return Collections.emptyMap();
    }

    @Override
    protected Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator() {
      return Collections.emptyIterator();
    }

    @Override
    protected Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator() {
      return new SingletonIterator<>(this);
    }

    @Override
    protected int getPreNodeFuzzyMatchTransitionSize() {
      return 1;
    }
  }

  /** The patternNode of the rawNode *. */
  private class OneLevelWildcardMatchNode extends SinglePathPatternNode {

    private final Function<String, Boolean> matchFunction;

    // Currently one level wildcard match supports extra matchFunctions
    private OneLevelWildcardMatchNode(
        final int patternIndex,
        final SinglePathPatternNode tracebackNode,
        final Function<String, Boolean> matchFunction) {
      super(patternIndex, tracebackNode);
      this.matchFunction = matchFunction;
    }

    @Override
    public boolean isMatch(final String event) {
      return matchFunction.apply(event);
    }

    @Override
    protected Map<String, IFATransition> getPreNodePreciseMatchTransition() {
      return Collections.emptyMap();
    }

    @Override
    protected Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator() {
      return Collections.emptyIterator();
    }

    @Override
    protected Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator() {
      if (tracebackNode == null) {
        return new SingletonIterator<>(this);
      } else {
        return new DualIterator<>(this, tracebackNode);
      }
    }

    @Override
    protected int getPreNodeFuzzyMatchTransitionSize() {
      if (tracebackNode == null) {
        return 1;
      } else {
        return 2;
      }
    }
  }

  /** The patternNode of the rawNode contains *, like d*. */
  private class RegexMatchNode extends SinglePathPatternNode {

    private Pattern regexPattern;

    private RegexMatchNode(int patternIndex, SinglePathPatternNode tracebackNode) {
      super(patternIndex, tracebackNode);
    }

    @Override
    public boolean isMatch(String event) {
      if (regexPattern == null) {
        regexPattern = Pattern.compile(pathPattern.getNodes()[patternIndex].replace("*", ".*"));
      }
      return regexPattern.matcher(event).matches();
    }

    @Override
    protected Map<String, IFATransition> getPreNodePreciseMatchTransition() {
      return Collections.emptyMap();
    }

    @Override
    protected Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator() {
      return Collections.emptyIterator();
    }

    @Override
    protected Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator() {
      if (tracebackNode == null) {
        return new SingletonIterator<>(this);
      } else {
        return new DualIterator<>(this, tracebackNode);
      }
    }

    @Override
    protected int getPreNodeFuzzyMatchTransitionSize() {
      if (tracebackNode == null) {
        return 1;
      } else {
        return 2;
      }
    }
  }

  /** The patternNode of the rawNode which is a specified name. */
  private class NameMatchNode extends SinglePathPatternNode {

    private Map<String, IFATransition> preNodePreciseMatchTransitionMap;

    private NameMatchNode(int patternIndex, SinglePathPatternNode tracebackNode) {
      super(patternIndex, tracebackNode);
    }

    @Override
    public boolean isMatch(String event) {
      return Objects.equals(pathPattern.getNodes()[patternIndex], event);
    }

    @Override
    protected Map<String, IFATransition> getPreNodePreciseMatchTransition() {
      if (preNodePreciseMatchTransitionMap == null) {
        preNodePreciseMatchTransitionMap =
            Collections.singletonMap(pathPattern.getNodes()[patternIndex], this);
      }
      return preNodePreciseMatchTransitionMap;
    }

    @Override
    protected Iterator<IFATransition> getPreNodePreciseMatchTransitionIterator() {
      return new SingletonIterator<>(this);
    }

    @Override
    protected Iterator<IFATransition> getPreNodeFuzzyMatchTransitionIterator() {
      if (tracebackNode == null) {
        return Collections.emptyIterator();
      } else {
        return new SingletonIterator<>(tracebackNode);
      }
    }

    @Override
    protected int getPreNodeFuzzyMatchTransitionSize() {
      if (tracebackNode == null) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  private static class SingletonIterator<E> implements Iterator<E> {

    private E e;

    private SingletonIterator(E e) {
      this.e = e;
    }

    @Override
    public boolean hasNext() {
      return e != null;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      E result = e;
      e = null;
      return result;
    }
  }

  private static class DualIterator<E> implements Iterator<E> {
    private E e1;
    private E e2;

    private DualIterator(E e1, E e2) {
      this.e1 = e1;
      this.e2 = e2;
    }

    @Override
    public boolean hasNext() {
      return e2 != null;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      E result;
      if (e1 != null) {
        result = e1;
        e1 = null;
      } else {
        result = e2;
        e2 = null;
      }
      return result;
    }
  }
}
