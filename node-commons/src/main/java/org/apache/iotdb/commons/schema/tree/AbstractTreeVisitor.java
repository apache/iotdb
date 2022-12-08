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
  private final Deque<VisitorStackEntry> visitorStack = new ArrayDeque<>();
  private final List<AncestorStackEntry> ancestorStack = new ArrayList<>();
  private int startIndexOfTraceback = -1;
  private IStateMatchInfo currentStateMatchInfo;
  private boolean shouldVisitSubtree;

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
    currentStateMatchInfo = new PreciseStateMatchInfo(rootState);
    visitorStack.push(new VisitorStackEntry(createChildrenIterator(root), 1));
    ancestorStack.add(new AncestorStackEntry(root, currentStateMatchInfo));
  }

  public void reset() {
    visitorStack.clear();
    ancestorStack.clear();
    nextMatchedNode = null;
    startIndexOfTraceback = -1;
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

      if (currentStateMatchInfo.hasFinalState()) {
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
        new VisitorStackEntry(createChildrenIterator(parent), visitorStack.peek().level + 1));
    ancestorStack.add(new AncestorStackEntry(parent, currentStateMatchInfo));
  }

  private Iterator<N> createChildrenIterator(N parent) {
    if (startIndexOfTraceback > -1) {
      return new TraceBackChildrenIterator(parent, currentStateMatchInfo);
    } else if (currentStateMatchInfo.hasOnlyPreciseMatchTransition()) {
      return new PreciseMatchChildrenIterator(parent, currentStateMatchInfo.getOneMatchedState());
    } else if (currentStateMatchInfo.hasNoPreciseMatchTransition()
        && currentStateMatchInfo.isSingleBatchMatchTransition()) {
      return new SingleBatchMatchChildrenIterator(
          parent, currentStateMatchInfo.getOneMatchedState());
    } else {
      return new MultiMatchTransitionChildrenIterator(
          parent, currentStateMatchInfo.getOneMatchedState());
    }
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
      return ancestorStack.get(ancestorStack.size() - 2).node;
    } else {
      return ancestorStack.get(ancestorStack.size() - 1).node;
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

  private interface IStateMatchInfo {

    boolean hasFinalState();

    boolean hasOnlyPreciseMatchTransition();

    boolean hasNoPreciseMatchTransition();

    boolean isSingleBatchMatchTransition();

    IFAState getOneMatchedState();

    Set<IFAState> getMatchedStateSet();

    Set<IFAState> getUncheckedSourceStateSet();

    void addMatchedState(Set<IFAState> stateSet);

    void addUncheckedSourceState(Set<IFAState> stateSet);

    void removeUncheckedSourceState(IFAState state);
  }

  private static class BatchStateMatchInfo implements IStateMatchInfo {

    private final Set<IFAState> matchedStateSet;

    /** SourceState, matched by parent */
    private Set<IFAState> uncheckedSourceStateSet;

    private boolean hasFinalState = false;

    BatchStateMatchInfo() {
      matchedStateSet = new HashSet<>();
      uncheckedSourceStateSet = Collections.emptySet();
    }

    BatchStateMatchInfo(Set<IFAState> matchedStateSet, Set<IFAState> uncheckedSourceStateSet) {
      this.matchedStateSet = matchedStateSet;
      for (IFAState state : matchedStateSet) {
        if (state.isFinal()) {
          hasFinalState = true;
          break;
        }
      }
      this.uncheckedSourceStateSet = uncheckedSourceStateSet;
    }

    @Override
    public boolean hasFinalState() {
      return hasFinalState;
    }

    @Override
    public boolean hasOnlyPreciseMatchTransition() {
      return false;
    }

    @Override
    public boolean hasNoPreciseMatchTransition() {
      return false;
    }

    @Override
    public boolean isSingleBatchMatchTransition() {
      return false;
    }

    @Override
    public IFAState getOneMatchedState() {
      return matchedStateSet.iterator().next();
    }

    @Override
    public Set<IFAState> getMatchedStateSet() {
      return matchedStateSet;
    }

    @Override
    public Set<IFAState> getUncheckedSourceStateSet() {
      return uncheckedSourceStateSet;
    }

    @Override
    public void addMatchedState(Set<IFAState> stateSet) {
      matchedStateSet.addAll(stateSet);
      if (hasFinalState) {
        return;
      }
      for (IFAState state : stateSet) {
        if (state.isFinal()) {
          hasFinalState = true;
          break;
        }
      }
    }

    @Override
    public void addUncheckedSourceState(Set<IFAState> stateSet) {
      if (uncheckedSourceStateSet.isEmpty()) {
        uncheckedSourceStateSet = stateSet;
      } else {
        uncheckedSourceStateSet.addAll(stateSet);
      }
    }

    @Override
    public void removeUncheckedSourceState(IFAState state) {
      uncheckedSourceStateSet.remove(state);
    }
  }

  private class PreciseStateMatchInfo implements IStateMatchInfo {

    private final IFAState matchedState;

    private PreciseStateMatchInfo(IFAState matchedState) {
      this.matchedState = matchedState;
    }

    @Override
    public boolean hasFinalState() {
      return matchedState.isFinal();
    }

    @Override
    public boolean hasOnlyPreciseMatchTransition() {
      return patternFA.getBatchMatchTransition(matchedState).isEmpty();
    }

    @Override
    public boolean hasNoPreciseMatchTransition() {
      return patternFA.getPreciseMatchTransition(matchedState).isEmpty();
    }

    @Override
    public boolean isSingleBatchMatchTransition() {
      return patternFA.getBatchMatchTransition(matchedState).size() == 1;
    }

    @Override
    public IFAState getOneMatchedState() {
      return matchedState;
    }

    @Override
    public Set<IFAState> getMatchedStateSet() {
      return Collections.singleton(matchedState);
    }

    @Override
    public Set<IFAState> getUncheckedSourceStateSet() {
      return Collections.emptySet();
    }

    @Override
    public void addMatchedState(Set<IFAState> stateSet) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addUncheckedSourceState(Set<IFAState> stateSet) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void removeUncheckedSourceState(IFAState state) {
      throw new UnsupportedOperationException();
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

    private final IStateMatchInfo stateMatchInfo;

    AncestorStackEntry(N node, IStateMatchInfo stateMatchInfo) {
      this.node = node;
      this.stateMatchInfo = stateMatchInfo;
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

    protected void saveResult(N child, IStateMatchInfo stateMatchInfo) {
      nextMatchedChild = child;
      currentStateMatchInfo = stateMatchInfo;
    }

    protected abstract void getNext();
  }

  private class PreciseMatchChildrenIterator extends AbstractChildrenIterator {
    private final N parent;
    private final IFAState sourceState;

    private final Iterator<IFATransition> transitionIterator;

    private PreciseMatchChildrenIterator(N parent, IFAState sourceState) {
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
            child, new PreciseStateMatchInfo(patternFA.getNextState(sourceState, transition)));
        return;
      }
    }
  }

  private class SingleBatchMatchChildrenIterator extends AbstractChildrenIterator {

    private final IFAState sourceState;

    private final IFATransition transition;

    private final PreciseStateMatchInfo stateMatchInfo;

    private final Iterator<N> childrenIterator;

    private SingleBatchMatchChildrenIterator(N parent, IFAState sourceState) {
      this.sourceState = sourceState;
      this.transition = patternFA.getTransition(sourceState).get(0);
      this.stateMatchInfo =
          new PreciseStateMatchInfo(patternFA.getNextState(sourceState, transition));
      this.childrenIterator = getChildrenIterator(parent);
    }

    @Override
    protected void getNext() {
      N child;
      while (childrenIterator.hasNext()) {
        child = childrenIterator.next();
        if (checkIsMatch(child, sourceState, transition)) {
          saveResult(child, stateMatchInfo);
          return;
        }
      }
    }
  }

  private class MultiMatchTransitionChildrenIterator extends AbstractChildrenIterator {

    private final IFAState sourceState;

    private final Iterator<N> iterator;

    private MultiMatchTransitionChildrenIterator(N parent, IFAState sourceState) {
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
        IStateMatchInfo stateMatchInfo;
        if (matchedStateSet.size() == 1) {
          stateMatchInfo = new PreciseStateMatchInfo(matchedStateSet.iterator().next());
        } else {
          stateMatchInfo = new BatchStateMatchInfo(matchedStateSet, Collections.emptySet());
          startIndexOfTraceback = ancestorStack.size();
        }

        saveResult(child, stateMatchInfo);
        return;
      }
    }
  }

  private class TraceBackChildrenIterator extends AbstractChildrenIterator {

    private final Iterator<N> iterator;

    private final IStateMatchInfo sourceStateMatchInfo;

    TraceBackChildrenIterator(N parent, IStateMatchInfo sourceStateMatchInfo) {
      this.sourceStateMatchInfo = sourceStateMatchInfo;
      this.iterator = getChildrenIterator(parent);
    }

    @Override
    protected void getNext() {
      N child;

      Set<IFAState> uncheckedSourceStateSet;
      Set<IFAState> matchedStateSet;
      IStateMatchInfo stateMatchInfo;

      while (iterator.hasNext()) {

        stateMatchInfo = null;
        uncheckedSourceStateSet = new HashSet<>(sourceStateMatchInfo.getMatchedStateSet());
        matchedStateSet = new HashSet<>();

        child = iterator.next();

        for (IFAState sourceState : sourceStateMatchInfo.getMatchedStateSet()) {
          checkAllTransitionOfOneSourceState(child, sourceState, matchedStateSet);
          uncheckedSourceStateSet.remove(sourceState);
          if (!matchedStateSet.isEmpty()) {
            break;
          }
        }

        if (!matchedStateSet.isEmpty()) {
          stateMatchInfo = new BatchStateMatchInfo(matchedStateSet, uncheckedSourceStateSet);
        } else {
          if (startIndexOfTraceback > -1) {
            stateMatchInfo = traceback(child);
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

  private IStateMatchInfo traceback(N node) {
    IStateMatchInfo result = new BatchStateMatchInfo();
    N currentNode;
    IStateMatchInfo currentStateMatchInfo, childStateMatchInfo;
    Deque<Iterator<IFAState>> stateIteratorStack;
    Iterator<IFAState> sourceIterator;
    IFAState sourceState;
    Set<IFAState> addedStateSet;
    for (int i = ancestorStack.size() - 1; i >= startIndexOfTraceback; i--) {
      currentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;

      // there's no state not further searched
      if (currentStateMatchInfo.getUncheckedSourceStateSet().isEmpty()) {
        continue;
      }

      // there's some state not further searched, select them
      stateIteratorStack = new ArrayDeque<>();
      stateIteratorStack.push(currentStateMatchInfo.getUncheckedSourceStateSet().iterator());

      // further search the selected state from current ancestor
      int index = i;
      while (!stateIteratorStack.isEmpty()) {
        sourceIterator = stateIteratorStack.peek();
        if (!sourceIterator.hasNext()) {
          stateIteratorStack.pop();
          index--;
          continue;
        }

        sourceState = sourceIterator.next();
        if (index == ancestorStack.size()) {
          currentStateMatchInfo = result;
          currentNode = node;
          childStateMatchInfo = null;
        } else if (index == ancestorStack.size() - 1) {
          currentStateMatchInfo = ancestorStack.get(index).stateMatchInfo;
          currentNode = ancestorStack.get(index).node;
          childStateMatchInfo = result;
        } else {
          currentStateMatchInfo = ancestorStack.get(index).stateMatchInfo;
          currentNode = ancestorStack.get(index).node;
          childStateMatchInfo = ancestorStack.get(index + 1).stateMatchInfo;
        }

        addedStateSet =
            getNewStatesFromAllTransitionOfOneSourceState(
                currentNode, sourceState, currentStateMatchInfo.getMatchedStateSet());
        if (addedStateSet.isEmpty()) {
          continue;
        }

        currentStateMatchInfo.addMatchedState(addedStateSet);
        currentStateMatchInfo.removeUncheckedSourceState(sourceState);
        if (currentNode == node) {
          return result;
        } else {
          childStateMatchInfo.addUncheckedSourceState(addedStateSet);
          stateIteratorStack.push(addedStateSet.iterator());
          index++;
        }
      }
    }
    return null;
  }

  protected IFATransition getMatchedTransition(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    return preciseMatchTransitionMap.get(node.getName());
  }

  protected boolean checkIsMatch(N node, IFAState sourceState, IFATransition transition) {
    return transition.isMatch(node.getName());
  }
}
