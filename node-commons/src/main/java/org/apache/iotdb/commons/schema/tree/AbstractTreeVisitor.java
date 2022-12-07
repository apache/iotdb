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
import org.apache.iotdb.commons.path.dfa.SimpleNFA;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

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
  private int startIndexOfTraceback = -1;
  protected boolean shouldVisitSubtree;

  protected StateMatchInfo currentStateMatchInfo;

  // result variables
  protected N nextMatchedNode;

  protected AbstractTreeVisitor(N root, PartialPath pathPattern, boolean isPrefixMatch) {
    this.root = root;

    this.patternFA = new SimpleNFA(pathPattern, isPrefixMatch);

    initStack();
  }

  private void initStack() {
    IFAState initialState = patternFA.getInitialState();
    IFAState rootState =
        patternFA.getNextState(
            initialState, patternFA.getPreciseMatchTransition(initialState).get(PATH_ROOT));
    StateMatchInfo rootStateMatchInfo = new StateMatchInfo(rootState, initialState);
    if (rootStateMatchInfo.hasBatchMatchTransition) {
      visitorStack.push(
          new VisitorStackEntry(new NonTraceBackChildrenIterator(root, rootState), 1));
    } else {
      visitorStack.push(new VisitorStackEntry(new PreciseChildrenIterator(root, rootState), 1));
    }

    ancestorStack.add(new AncestorStackEntry(root, rootStateMatchInfo));
  }

  public void reset() {
    visitorStack.clear();
    ancestorStack.clear();
    nextMatchedNode = null;
    initStack();
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
    R result = generateResult();
    nextMatchedNode = null;
    return result;
  }

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
    Iterator<N> childrenIterator;
    if (startIndexOfTraceback > -1) {
      childrenIterator = new TraceBackChildrenIterator(parent, currentStateMatchInfo);
    } else if (currentStateMatchInfo.hasBatchMatchTransition) {
      childrenIterator =
          new NonTraceBackChildrenIterator(
              parent, currentStateMatchInfo.matchedStateSet.iterator().next());
    } else {
      childrenIterator =
          new PreciseChildrenIterator(
              parent, currentStateMatchInfo.matchedStateSet.iterator().next());
    }
    visitorStack.push(new VisitorStackEntry(childrenIterator, visitorStack.peek().level + 1));
    ancestorStack.add(new AncestorStackEntry(parent, currentStateMatchInfo));
  }

  private void popStack() {
    visitorStack.pop();
    // The ancestor pop operation with level check supports the children of one node pushed by
    // batch.
    if (!visitorStack.isEmpty() && visitorStack.peek().level < ancestorStack.size()) {
      ancestorStack.remove(ancestorStack.size() - 1);
      if (ancestorStack.size() == startIndexOfTraceback) {
        startIndexOfTraceback = -1;
      }
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

  protected N getParentOfNextMatchedNode() {
    if (shouldVisitSubtree) {
      return ancestorStack.get(ancestorStack.size() - 2).getNode();
    } else {
      return ancestorStack.get(ancestorStack.size() - 1).getNode();
    }
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

  private class StateMatchInfo {

    private final Set<IFAState> matchedStateSet;

    /** SourceState, matched by parent */
    private final Set<IFAState> checkedSourceStateSet;

    private boolean hasBatchMatchTransition = false;

    private boolean hasFinalState = false;

    StateMatchInfo() {
      matchedStateSet = new HashSet<>();
      checkedSourceStateSet = new HashSet<>();
    }

    StateMatchInfo(IFAState matchedState, IFAState sourceState) {
      this.matchedStateSet = Collections.singleton(matchedState);
      this.checkedSourceStateSet = Collections.singleton(sourceState);
      if (matchedState.isFinal()) {
        hasFinalState = true;
      }
      if (patternFA.getBatchMatchTransition(matchedState).size() > 0) {
        hasBatchMatchTransition = true;
      }
    }

    StateMatchInfo(Set<IFAState> matchedStateSet, Set<IFAState> checkedSourceStateSet) {
      this.matchedStateSet = matchedStateSet;
      for (IFAState state : matchedStateSet) {
        if (state.isFinal()) {
          hasFinalState = true;
        }
        if (patternFA.getBatchMatchTransition(state).size() > 0) {
          hasBatchMatchTransition = true;
        }
      }
      this.checkedSourceStateSet = checkedSourceStateSet;
    }

    private void addState(Set<IFAState> stateSet) {
      matchedStateSet.addAll(stateSet);
      for (IFAState state : stateSet) {
        if (state.isFinal()) {
          hasFinalState = true;
        }
        if (patternFA.getBatchMatchTransition(state).size() > 0) {
          hasBatchMatchTransition = true;
        }
      }
    }
  }

  private class VisitorStackEntry {

    private final Iterator<N> iterator;

    private final int level;

    VisitorStackEntry(Iterator<N> iterator, int level) {
      this.iterator = iterator;
      this.level = level;
    }
  }

  private class AncestorStackEntry {
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

  private abstract class AbstractChildrenIterator implements Iterator<N> {

    private N nextMatchedChild;

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

    protected void saveResult(N child, StateMatchInfo stateMatchInfo) {
      nextMatchedChild = child;
      currentStateMatchInfo = stateMatchInfo;
    }

    protected abstract void getNext();
  }

  private class PreciseChildrenIterator extends AbstractChildrenIterator {
    private final N parent;
    private final IFAState sourceState;

    private final Iterator<IFATransition> transitionIterator;

    private PreciseChildrenIterator(N parent, IFAState sourceState) {
      this.parent = parent;
      this.sourceState = sourceState;
      transitionIterator = patternFA.getPreciseMatchTransition(sourceState).values().iterator();
    }

    @Override
    protected void getNext() {
      N child;
      IFATransition transition;
      while (transitionIterator.hasNext()) {
        transition = transitionIterator.next();
        child = getChild(parent, transition.getValue());
        if (child == null) {
          continue;
        }
        saveResult(
            child,
            new StateMatchInfo(patternFA.getNextState(sourceState, transition), sourceState));
        return;
      }
    }
  }

  private class NonTraceBackChildrenIterator extends AbstractChildrenIterator {

    private final IFAState sourceState;

    private final Iterator<N> iterator;

    private NonTraceBackChildrenIterator(N parent, IFAState sourceState) {
      this.sourceState = sourceState;
      this.iterator = getChildrenIterator(parent);
    }

    @Override
    protected void getNext() {
      N child;

      Set<IFAState> matchedStateSet = new HashSet<>();

      while (iterator.hasNext()) {
        child = iterator.next();
        checkAllTransitionOfOneSourceState(child, sourceState, matchedStateSet);
        if (matchedStateSet.isEmpty()) {
          continue;
        }
        StateMatchInfo stateMatchInfo =
            new StateMatchInfo(matchedStateSet, Collections.singleton(sourceState));
        if (stateMatchInfo.matchedStateSet.size() > 1) {
          startIndexOfTraceback = ancestorStack.size();
        }
        saveResult(child, stateMatchInfo);
        return;
      }
    }
  }

  private class TraceBackChildrenIterator extends AbstractChildrenIterator {

    private final Iterator<N> iterator;

    private final StateMatchInfo sourceStateMatchInfo;

    TraceBackChildrenIterator(N parent, StateMatchInfo sourceStateMatchInfo) {
      this.sourceStateMatchInfo = sourceStateMatchInfo;
      this.iterator = getChildrenIterator(parent);
    }

    @Override
    protected void getNext() {
      N child;

      Set<IFAState> checkedSourceStateSet = new HashSet<>();
      Set<IFAState> matchedStateSet = new HashSet<>();
      StateMatchInfo stateMatchInfo = null;

      while (iterator.hasNext()) {
        child = iterator.next();
        for (IFAState sourceState : sourceStateMatchInfo.matchedStateSet) {
          checkAllTransitionOfOneSourceState(child, sourceState, matchedStateSet);
          checkedSourceStateSet.add(sourceState);
          if (!matchedStateSet.isEmpty()) {
            break;
          }
        }

        if (!matchedStateSet.isEmpty()) {
          stateMatchInfo = new StateMatchInfo(matchedStateSet, checkedSourceStateSet);
        } else {
          if (startIndexOfTraceback > -1) {
            stateMatchInfo = traceback(child, sourceStateMatchInfo);
            if (!stateMatchInfo.matchedStateSet.isEmpty()) {
              saveResult(child, stateMatchInfo);
            }
          }
        }

        if (stateMatchInfo != null) {
          if (startIndexOfTraceback == -1) {
            startIndexOfTraceback = ancestorStack.size();
          }
          saveResult(child, stateMatchInfo);
          return;
        }
      }
    }
  }

  private void checkAllTransitionOfOneSourceState(
      N child, IFAState sourceState, Set<IFAState> matchedStateSet) {
    Map<String, IFATransition> preciseMatchTransitionMap =
        patternFA.getPreciseMatchTransition(sourceState);
    List<IFATransition> batchMatchTransitionList = patternFA.getBatchMatchTransition(sourceState);

    if (!preciseMatchTransitionMap.isEmpty()) {
      IFATransition targetTransition =
          getMatchedTransition(child, sourceState, preciseMatchTransitionMap);
      if (targetTransition != null) {
        matchedStateSet.add(patternFA.getNextState(sourceState, targetTransition));
      }
    }
    for (IFATransition transition : batchMatchTransitionList) {
      if (checkIsMatch(child, sourceState, transition)) {
        matchedStateSet.add(patternFA.getNextState(sourceState, transition));
      }
    }
  }

  private Set<IFAState> getNewStatesFromAllTransitionOfOneSourceState(
      N child, IFAState sourceState, Set<IFAState> alreadyMatchedStateSet) {
    Set<IFAState> newStateSet = new HashSet<>();
    Map<String, IFATransition> preciseMatchTransitionMap =
        patternFA.getPreciseMatchTransition(sourceState);
    List<IFATransition> batchMatchTransitionList = patternFA.getBatchMatchTransition(sourceState);

    IFAState matchedState;

    if (!preciseMatchTransitionMap.isEmpty()) {
      IFATransition targetTransition =
          getMatchedTransition(child, sourceState, preciseMatchTransitionMap);
      if (targetTransition != null) {
        matchedState = patternFA.getNextState(sourceState, targetTransition);
        if (!alreadyMatchedStateSet.contains(matchedState)) {
          newStateSet.add(matchedState);
        }
      }
    }
    for (IFATransition transition : batchMatchTransitionList) {
      if (checkIsMatch(child, sourceState, transition)) {
        matchedState = patternFA.getNextState(sourceState, transition);
        if (alreadyMatchedStateSet.contains(matchedState)) {
          continue;
        }
        newStateSet.add(matchedState);
      }
    }
    return newStateSet;
  }

  private StateMatchInfo traceback(N node, StateMatchInfo currentStateMatchInfo) {
    StateMatchInfo result = new StateMatchInfo();
    result.checkedSourceStateSet.addAll(currentStateMatchInfo.matchedStateSet);
    N childNode;
    StateMatchInfo parentStateMatchInfo = result, childStateMatchInfo;
    Set<IFAState> addedStateSet;
    for (int i = ancestorStack.size() - 1; i >= startIndexOfTraceback; i--) {
      childStateMatchInfo = parentStateMatchInfo;
      parentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;
      if (parentStateMatchInfo.matchedStateSet.size() == 1) {
        continue;
      }
      addedStateSet = new HashSet<>();
      for (IFAState sourceState : parentStateMatchInfo.matchedStateSet) {
        if (!childStateMatchInfo.checkedSourceStateSet.contains(sourceState)) {
          addedStateSet.add(sourceState);
        }
      }

      // there's no state not further searched
      if (addedStateSet.isEmpty()) {
        continue;
      }

      // there's some state not further searched, select them
      Deque<Iterator<IFAState>> stateIteratorStack = new ArrayDeque<>();
      stateIteratorStack.push(addedStateSet.iterator());

      // further search the selected state from current ancestor
      Iterator<IFAState> sourceIterator;
      IFAState sourceState;
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

        addedStateSet =
            getNewStatesFromAllTransitionOfOneSourceState(
                childNode, sourceState, childStateMatchInfo.matchedStateSet);
        childStateMatchInfo.addState(addedStateSet);
        childStateMatchInfo.checkedSourceStateSet.add(sourceState);

        if (!result.matchedStateSet.isEmpty()) {
          return result;
        }
        if (!addedStateSet.isEmpty()) {
          stateIteratorStack.push(addedStateSet.iterator());
          index++;
        }
      }
    }
    return result;
  }

  protected IFATransition getMatchedTransition(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    return preciseMatchTransitionMap.get(node.getName());
  }

  protected boolean checkIsMatch(N node, IFAState sourceState, IFATransition transition) {
    return transition.isMatch(node.getName());
  }
}
