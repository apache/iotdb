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

package org.apache.iotdb.commons.schema.tree;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.dfa.IFAState;
import org.apache.iotdb.commons.path.dfa.IFATransition;
import org.apache.iotdb.commons.path.dfa.IPatternFA;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * This class defines a dfs-based algorithm of tree-traversing with path pattern match, and support
 * iterating each element of the result.
 *
 * <p>This class takes three basis parameters as input:
 *
 * <ol>
 *   <li>N root: the root node of the tree to be traversed.
 *   <li>PartialPath patPattern: the pattern of path that the path of target element matches
 *   <li>boolean isPrefixMatch: whether the pathPattern is used for matching the prefix; if so, all
 *       elements with path starting with the matched prefix will be collected
 * </ol>
 *
 * <p>If any tree wants to integrate and use this class. The following steps must be attained:
 *
 * <ol>
 *   <li>The node of the tree must implement ITreeNode interface and the generic N should be defined
 *       as the node class.
 *   <li>The result type R should be defined.
 *   <li>Implement the abstract methods, and for the concrete requirements, please refer to the
 *       javadoc of specific method.
 * </ol>
 *
 * @param <N> The node consisting the tree.
 * @param <R> The result extracted from the tree.
 */
public abstract class AbstractTreeVisitor<N extends ITreeNode, R> implements Iterator<R> {

  // command parameters
  protected final N root;

  protected final IPatternFA patternFA;

  // run time variables
  protected final Deque<VisitorStackEntry> visitorStack = new ArrayDeque<>();
  protected final List<AncestorStackEntry> ancestorStack = new ArrayList<>();
  protected boolean shouldVisitSubtree;

  protected StateMatchInfo currentStateMatchInfo;

  // result variables
  protected N nextMatchedNode;

  protected AbstractTreeVisitor(N root, PartialPath pathPattern, boolean isPrefixMatch) {
    this.root = root;

    this.patternFA = new SimpleNFA(pathPattern, isPrefixMatch);

    IFAState initialState = patternFA.getInitialState();
    visitorStack.push(
        new VisitorStackEntry(
            new ChildrenIterator(Collections.singletonList(root).iterator(), initialState), 0));
  }

  @Override
  public boolean hasNext() {
    if (nextMatchedNode == null) {
      getNext();
    }
    return nextMatchedNode != null;
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return consumeNextMatchedNode();
  }

  private R consumeNextMatchedNode() {
    R result = generateResult();
    nextMatchedNode = null;
    return result;
  }

  /**
   * Basically, the algorithm traverse the tree with dfs strategy. When it comes to push children
   * into stack, the path pattern will be used to filter the children.
   *
   * <p>When there's MULTI_LEVEL_WILDCARD in given path pattern. There are the following notices:
   *
   * <ol>
   *   <li>When it comes to push children into stack and there's MULTI_LEVEL_WILDCARD before the
   *       current patternIndex, all the children will be pushed.
   *   <li>When a node cannot match the target node name in the patternIndex place and there's
   *       MULTI_LEVEL_WILDCARD before the current patternIndex, the node names between the current
   *       patternIndex and lastMultiLevelWildcardIndex will be checked until the partial path end
   *       with current node can match one. The children will be pushed with the matched index + 1.
   * </ol>
   *
   * <p>Each node and fullPath of the tree will be traversed at most once.
   */
  protected void getNext() {
    nextMatchedNode = null;
    VisitorStackEntry stackEntry;
    N node;
    Iterator<N> iterator;
    while (!visitorStack.isEmpty()) {
      stackEntry = visitorStack.peek();
      iterator = stackEntry.iterator;

      if (!iterator.hasNext()) {
        popStack();
        continue;
      }

      node = iterator.next();

      if (currentStateMatchInfo.hasFinalState) {
        shouldVisitSubtree = processFullMatchedNode(node) && isInternalNode(node);
      } else {
        shouldVisitSubtree = processInternalMatchedNode(node) && isInternalNode(node);
      }

      if (shouldVisitSubtree) {
        pushChildren(node);
      }

      if (nextMatchedNode != null) {
        return;
      }
    }
  }

  private void pushChildren(N parent) {
    visitorStack.push(
        new VisitorStackEntry(
            new ChildrenIterator(parent, currentStateMatchInfo), visitorStack.peek().level + 1));
    ancestorStack.add(new AncestorStackEntry(parent, currentStateMatchInfo));
  }

  public void reset() {
    visitorStack.clear();
    ancestorStack.clear();
    nextMatchedNode = null;
    IFAState initialState = patternFA.getInitialState();
    visitorStack.push(
        new VisitorStackEntry(
            new ChildrenIterator(Collections.singletonList(root).iterator(), initialState), 0));
  }

  private void popStack() {
    visitorStack.pop();
    // The ancestor pop operation with level check supports the children of one node pushed by
    // batch.
    if (!visitorStack.isEmpty() && visitorStack.peek().level < ancestorStack.size()) {
      ancestorStack.remove(ancestorStack.size() - 1);
    }
  }

  protected String[] generateFullPathNodes() {
    List<String> nodeNames = new ArrayList<>();
    Iterator<AncestorStackEntry> iterator = ancestorStack.iterator();
    for (int i = 0, size = shouldVisitSubtree ? ancestorStack.size() - 1 : ancestorStack.size();
        i < size;
        i++) {
      if (iterator.hasNext()) {
        nodeNames.add(iterator.next().node.getName());
      }
    }
    nodeNames.add(nextMatchedNode.getName());
    return nodeNames.toArray(new String[0]);
  }

  /**
   * Check whether the given node is an internal node of this tree. Return true if the given node is
   * an internal node. Return false if the given node is a leaf node.
   */
  protected abstract boolean isInternalNode(N node);

  // Get a child with the given childName.
  protected abstract N getChild(N parent, String childName);

  // Get a iterator of all children.
  protected abstract Iterator<N> getChildrenIterator(N parent);

  /**
   * Internal-match means the node matches an internal node name of the given path pattern. root.sg
   * internal match root.sg.**(pattern). This method should be implemented according to concrete
   * tasks.
   *
   * <p>Return whether the subtree of given node should be processed. If return true, the traversing
   * process will keep traversing the subtree. If return false, the traversing process will skip the
   * subtree of given node.
   */
  protected abstract boolean processInternalMatchedNode(N node);

  /**
   * Full-match means the node matches the last node name of the given path pattern. root.sg.d full
   * match root.sg.**(pattern) This method should be implemented according to concrete tasks.
   *
   * <p>Return whether the subtree of given node should be processed. If return true, the traversing
   * process will keep traversing the subtree. If return false, the traversing process will skip the
   * subtree of given node.
   */
  protected abstract boolean processFullMatchedNode(N node);

  /** The method used for generating the result based on the matched node. */
  protected abstract R generateResult();

  protected IFATransition getMatchedTransition(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    return preciseMatchTransitionMap.get(node.getName());
  }

  protected boolean checkIsMatch(N node, IFAState sourceState, IFATransition transition) {
    return transition.isMatch(node.getName());
  }

  private StateMatchInfo traceback(N node, StateMatchInfo currentStateMatchInfo) {
    StateMatchInfo result = new StateMatchInfo();
    for (IFAState currentState : currentStateMatchInfo.matchedStateSet) {
      result.stateMatchDetail.put(currentState, null);
    }
    N childNode;
    StateMatchInfo parentStateMatchInfo = result, childStateMatchInfo;
    Set<IFAState> addedState;
    Map<IFATransition, IFAState> transitionDetail;
    for (int i = ancestorStack.size() - 1, end = currentStateMatchInfo.indexOfTraceback;
        i >= end;
        i--) {
      childStateMatchInfo = parentStateMatchInfo;
      parentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;
      addedState = new HashSet<>();
      for (IFAState sourceState : parentStateMatchInfo.matchedStateSet) {
        if (!childStateMatchInfo.stateMatchDetail.containsKey(sourceState)) {
          addedState.add(sourceState);
        }
      }

      // there's no state not further searched
      if (addedState.isEmpty()) {
        continue;
      }

      // there's some state not further searched, select them
      Deque<Iterator<IFAState>> stateIteratorStack = new ArrayDeque<>();
      stateIteratorStack.push(addedState.iterator());

      // further search the selected state from current ancestor
      Iterator<IFAState> sourceIterator;
      IFAState sourceState;
      IFAState matchedState;
      int index = i;
      while (!stateIteratorStack.isEmpty()) {
        sourceIterator = stateIteratorStack.peek();
        if (!sourceIterator.hasNext()) {
          stateIteratorStack.pop();
          index--;
          continue;
        }

        sourceState = sourceIterator.next();
        if (index == ancestorStack.size() - 1) {
          childStateMatchInfo = result;
          childNode = node;
        } else {
          childStateMatchInfo = ancestorStack.get(index + 1).stateMatchInfo;
          childNode = ancestorStack.get(index + 1).node;
        }

        addedState = new HashSet<>();
        transitionDetail = new HashMap<>();
        for (IFATransition transition : patternFA.getTransition(sourceState)) {
          if (checkIsMatch(childNode, sourceState, transition)) {
            matchedState = patternFA.getNextState(sourceState, transition);
            if (childStateMatchInfo.matchedStateSet.contains(matchedState)) {
              continue;
            }
            addedState.add(matchedState);
            childStateMatchInfo.addState(matchedState);
            transitionDetail.put(transition, matchedState);
          }
        }
        childStateMatchInfo.stateMatchDetail.put(
            sourceState, transitionDetail.isEmpty() ? null : transitionDetail);
        if (!result.matchedStateSet.isEmpty()) {
          return result;
        }
        if (!addedState.isEmpty()) {
          stateIteratorStack.push(addedState.iterator());
          index++;
        }
      }
    }
    return result;
  }

  protected class ChildrenIterator implements Iterator<N> {

    private final Iterator<N> iterator;

    private final StateMatchInfo sourceStateMatchInfo;

    private N nextMatchedChild;

    ChildrenIterator(N parent, StateMatchInfo sourceStateMatchInfo) {
      this.sourceStateMatchInfo = sourceStateMatchInfo;

      if (sourceStateMatchInfo.indexOfTraceback > -1
          || sourceStateMatchInfo.hasBatchMatchTransition) {
        this.iterator = getChildrenIterator(parent);
      } else {
        List<N> list = new ArrayList<>();
        N child;
        for (IFAState sourceState : sourceStateMatchInfo.matchedStateSet) {
          for (String childName :
              sourceStateMatchInfo.preciseMatchTransitionsMap.get(sourceState).keySet()) {
            child = getChild(parent, childName);
            if (child != null) {
              list.add(child);
            }
          }
        }
        iterator = list.iterator();
      }
    }

    ChildrenIterator(Iterator<N> iterator, IFAState sourceState) {
      this.sourceStateMatchInfo = new StateMatchInfo();
      sourceStateMatchInfo.addState(sourceState);
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      if (nextMatchedChild == null) {
        getNext();
      }
      return nextMatchedChild != null;
    }

    @Override
    public N next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      N result = nextMatchedChild;
      nextMatchedChild = null;
      return result;
    }

    private void getNext() {
      N child;

      Map<String, IFATransition> preciseMatchTransitionMap;
      List<IFATransition> batchMatchTransitionList;

      IFATransition targetTransition;

      boolean hasResult = false;
      IFAState matchedState;
      Map<IFAState, Map<IFATransition, IFAState>> stateMatchDetail = new HashMap<>();
      Map<IFATransition, IFAState> transitionDetail;
      Set<IFAState> matchedStateSet = new HashSet<>();

      while (iterator.hasNext()) {
        child = iterator.next();
        for (IFAState sourceState : sourceStateMatchInfo.matchedStateSet) {
          preciseMatchTransitionMap =
              sourceStateMatchInfo.preciseMatchTransitionsMap.get(sourceState);
          batchMatchTransitionList = sourceStateMatchInfo.batchMatchTransitionsMap.get(sourceState);

          transitionDetail = new HashMap<>();

          if (!preciseMatchTransitionMap.isEmpty()) {
            targetTransition = getMatchedTransition(child, sourceState, preciseMatchTransitionMap);
            if (targetTransition != null) {
              hasResult = true;
              matchedState = patternFA.getNextState(sourceState, targetTransition);
              matchedStateSet.add(matchedState);
              transitionDetail.put(
                  targetTransition, patternFA.getNextState(sourceState, targetTransition));
            }
            if (hasResult) {
              for (IFATransition transition : preciseMatchTransitionMap.values()) {
                if (transition.equals(targetTransition)) {
                  continue;
                }
                if (checkIsMatch(child, sourceState, transition)) {
                  matchedState = patternFA.getNextState(sourceState, transition);
                  matchedStateSet.add(matchedState);
                  transitionDetail.put(transition, matchedState);
                }
              }
            }
          }
          for (IFATransition transition : batchMatchTransitionList) {
            if (checkIsMatch(child, sourceState, transition)) {
              hasResult = true;
              matchedState = patternFA.getNextState(sourceState, transition);
              matchedStateSet.add(matchedState);
              transitionDetail.put(transition, matchedState);
            }
          }
          stateMatchDetail.put(sourceState, transitionDetail.isEmpty() ? null : transitionDetail);
          if (hasResult) {
            break;
          }
        }

        if (hasResult) {
          currentStateMatchInfo = new StateMatchInfo(stateMatchDetail, matchedStateSet);
          nextMatchedChild = child;
        } else {
          if (sourceStateMatchInfo.indexOfTraceback > -1) {
            StateMatchInfo stateMatchInfo = traceback(child, sourceStateMatchInfo);
            if (!stateMatchInfo.matchedStateSet.isEmpty()) {
              hasResult = true;
              currentStateMatchInfo = stateMatchInfo;
              nextMatchedChild = child;
            }
          }
        }
        if (hasResult) {
          if (sourceStateMatchInfo.indexOfTraceback > -1) {
            currentStateMatchInfo.indexOfTraceback = sourceStateMatchInfo.indexOfTraceback;
          } else if (currentStateMatchInfo.matchedStateSet.size() > 1) {
            currentStateMatchInfo.indexOfTraceback = ancestorStack.size();
          }
          return;
        }
      }
    }
  }

  protected class VisitorStackEntry {

    private final ChildrenIterator iterator;

    private final int level;

    VisitorStackEntry(ChildrenIterator iterator, int level) {
      this.iterator = iterator;
      this.level = level;
    }
  }

  protected class AncestorStackEntry {
    private final N node;

    private final StateMatchInfo stateMatchInfo;

    AncestorStackEntry(N node, StateMatchInfo stateMatchInfo) {
      this.node = node;
      this.stateMatchInfo = stateMatchInfo;
    }

    public N getNode() {
      return node;
    }
  }

  private class StateMatchInfo {
    /**
     * SourceState, matched by parent -> transitions matched by this node -> state matched by this
     * node
     */
    private final Map<IFAState, Map<IFATransition, IFAState>> stateMatchDetail;

    private final Set<IFAState> matchedStateSet;

    private final Map<IFAState, Map<String, IFATransition>> preciseMatchTransitionsMap =
        new HashMap<>();

    private final Map<IFAState, List<IFATransition>> batchMatchTransitionsMap = new HashMap<>();

    private boolean hasBatchMatchTransition = false;

    private boolean hasFinalState = false;

    private int indexOfTraceback = -1;

    StateMatchInfo() {
      stateMatchDetail = new HashMap<>();
      matchedStateSet = new HashSet<>();
    }

    StateMatchInfo(
        Map<IFAState, Map<IFATransition, IFAState>> stateMatchDetail,
        Set<IFAState> matchedStateSet) {
      this.stateMatchDetail = stateMatchDetail;
      this.matchedStateSet = matchedStateSet;
      for (IFAState state : matchedStateSet) {
        if (state.isFinal()) {
          hasFinalState = true;
        }
        processTransitionOfState(state);
      }
    }

    private void addState(IFAState state) {
      matchedStateSet.add(state);
      if (state.isFinal()) {
        hasFinalState = true;
      }
      processTransitionOfState(state);
    }

    private void processTransitionOfState(IFAState state) {
      List<IFATransition> transitionList = patternFA.getTransition(state);
      Map<String, IFATransition> preciseMatchTransitions = new HashMap<>();
      List<IFATransition> batchMatchTransitions = new ArrayList<>();
      for (IFATransition transition : transitionList) {
        if (transition.isBatch()) {
          batchMatchTransitions.add(transition);
        } else {
          preciseMatchTransitions.put(transition.getValue(), transition);
        }
      }
      preciseMatchTransitionsMap.put(state, preciseMatchTransitions);
      batchMatchTransitionsMap.put(state, batchMatchTransitions);
      if (!batchMatchTransitions.isEmpty()) {
        hasBatchMatchTransition = true;
      }
    }
  }

  private static class SimpleNFA implements IPatternFA {

    private final String[] nodes;
    private final boolean isPrefixMatch;

    private final SimpleNFAState[] states;

    private final SimpleNFAState initialState = new SimpleNFAState(-1);

    SimpleNFA(PartialPath pathPattern, boolean isPrefixMatch) {
      this.nodes = optimizePathPattern(pathPattern);
      this.isPrefixMatch = isPrefixMatch;

      states = new SimpleNFAState[this.nodes.length + 1];
      for (int i = 0; i < states.length; i++) {
        states[i] = new SimpleNFAState(i);
      }
    }

    /**
     * Optimize the given path pattern. Currently, the node name used for one level match will be
     * transformed into a regex. e.g. given pathPattern {"root", "sg", "d*", "s"} and the
     * optimizedPathPattern is {"root", "sg", "d.*", "s"}.
     */
    private String[] optimizePathPattern(PartialPath pathPattern) {
      String[] rawNodes = pathPattern.getNodes();
      List<String> optimizedNodes = new ArrayList<>(rawNodes.length);
      for (String rawNode : rawNodes) {
        if (rawNode.equals(MULTI_LEVEL_PATH_WILDCARD)) {
          optimizedNodes.add(MULTI_LEVEL_PATH_WILDCARD);
        } else if (rawNode.contains(ONE_LEVEL_PATH_WILDCARD)) {
          optimizedNodes.add(rawNode.replace("*", ".*"));
        } else {
          optimizedNodes.add(rawNode);
        }
      }

      return optimizedNodes.toArray(new String[0]);
    }

    @Override
    public List<IFATransition> getTransition(IFAState state) {
      SimpleNFAState nfaState = (SimpleNFAState) state;
      if (nfaState.isInitial()) {
        return Collections.singletonList(states[0]);
      }
      if (nfaState.patternIndex == nodes.length) {
        // prefix match
        return Collections.singletonList(nfaState);
      }

      if (nfaState.patternIndex == nodes.length - 1) {
        if (isPrefixMatch) {
          return Collections.singletonList(states[nodes.length]);
        } else if (nodes[nfaState.patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
          return Collections.singletonList(nfaState);
        }
        {
          return Collections.emptyList();
        }
      }

      if (nodes[nfaState.patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return Arrays.asList(states[nfaState.patternIndex + 1], nfaState);
      } else {
        return Collections.singletonList(states[nfaState.patternIndex + 1]);
      }
    }

    @Override
    public IFAState getNextState(IFAState currentState, IFATransition transition) {
      return (SimpleNFAState) transition;
    }

    @Override
    public IFAState getInitialState() {
      return initialState;
    }

    private class SimpleNFAState implements IFAState, IFATransition {

      private final int patternIndex;

      SimpleNFAState(int patternIndex) {
        this.patternIndex = patternIndex;
      }

      @Override
      public boolean isInitial() {
        return patternIndex == -1;
      }

      @Override
      public boolean isFinal() {
        return patternIndex >= nodes.length - 1;
      }

      @Override
      public int getIndex() {
        return patternIndex;
      }

      @Override
      public String getValue() {
        return nodes[patternIndex];
      }

      @Override
      public boolean isMatch(String event) {
        if (patternIndex == nodes.length) {
          return true;
        }
        if (nodes[patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
          return true;
        }
        if (nodes[patternIndex].equals(ONE_LEVEL_PATH_WILDCARD)) {
          return true;
        }
        if (nodes[patternIndex].contains(ONE_LEVEL_PATH_WILDCARD)) {
          return Pattern.matches(nodes[patternIndex], event);
        }
        return nodes[patternIndex].equals(event);
      }

      @Override
      public boolean isBatch() {
        return patternIndex == nodes.length
            || nodes[patternIndex].contains(ONE_LEVEL_PATH_WILDCARD);
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleNFAState nfaState = (SimpleNFAState) o;
        return patternIndex == nfaState.patternIndex;
      }

      @Override
      public int hashCode() {
        return Objects.hash(patternIndex);
      }
    }
  }
}
