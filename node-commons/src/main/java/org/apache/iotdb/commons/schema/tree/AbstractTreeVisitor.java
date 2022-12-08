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
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
        shouldVisitSubtree = processFullMatchedNode(node);
      } else {
        shouldVisitSubtree = processInternalMatchedNode(node);
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

    MatchedStateSet getMatchedStateSet();

    UncheckedSourceStateQueue getUncheckedSourceStateQueue();

    void setHasFinalState();

    void addUncheckedSourceState(int start, MatchedStateSet stateSet);

    void removeUncheckedSourceState(IFAState state);
  }

  private class BatchStateMatchInfo implements IStateMatchInfo {

    private final MatchedStateSet matchedStateSet;

    /** SourceState, matched by parent */
    private final UncheckedSourceStateQueue uncheckedSourceStateQueue;

    private boolean hasFinalState = false;

    BatchStateMatchInfo() {
      matchedStateSet = new MatchedStateSet(patternFA.getStateSize());
      uncheckedSourceStateQueue = new UncheckedSourceStateQueue();
    }

    BatchStateMatchInfo(
        MatchedStateSet matchedStateSet,
        UncheckedSourceStateQueue uncheckedSourceStateQueue,
        boolean hasFinalState) {
      this.matchedStateSet = matchedStateSet;
      this.uncheckedSourceStateQueue = uncheckedSourceStateQueue;
      this.hasFinalState = hasFinalState;
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
      throw new UnsupportedOperationException();
    }

    @Override
    public MatchedStateSet getMatchedStateSet() {
      return matchedStateSet;
    }

    @Override
    public UncheckedSourceStateQueue getUncheckedSourceStateQueue() {
      return uncheckedSourceStateQueue;
    }

    @Override
    public void setHasFinalState() {
      hasFinalState = true;
    }

    @Override
    public void addUncheckedSourceState(int start, MatchedStateSet stateSet) {
      if (uncheckedSourceStateQueue.isEmpty()) {
        uncheckedSourceStateQueue.addAll(start, stateSet);
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public void removeUncheckedSourceState(IFAState state) {
      uncheckedSourceStateQueue.remove(state);
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
    public MatchedStateSet getMatchedStateSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public UncheckedSourceStateQueue getUncheckedSourceStateQueue() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setHasFinalState() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addUncheckedSourceState(int start, MatchedStateSet stateSet) {
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
        if (tryGetNextState(child, sourceState, transition) == null) {
          continue;
        }
        saveResult(child, stateMatchInfo);
        return;
      }
    }
  }

  private class MultiMatchTransitionChildrenIterator extends AbstractChildrenIterator {

    private final IFAState sourceState;

    private final Map<String, IFATransition> preciseMatchTransitionMap;

    private final List<IFATransition> batchMatchTransitionList;

    private final Iterator<N> iterator;

    private MultiMatchTransitionChildrenIterator(N parent, IFAState sourceState) {
      this.sourceState = sourceState;
      this.iterator = getChildrenIterator(parent);
      this.preciseMatchTransitionMap = patternFA.getPreciseMatchTransition(sourceState);
      this.batchMatchTransitionList = patternFA.getBatchMatchTransition(sourceState);
    }

    @Override
    protected void getNext() {
      N child;

      IFAState firstMatchedState = null;
      MatchedStateSet matchedStateSet = null;
      boolean hasFinalState = false;

      IFAState matchedState;
      while (iterator.hasNext()) {
        child = iterator.next();

        if (!preciseMatchTransitionMap.isEmpty()) {
          firstMatchedState = tryGetNextState(child, sourceState, preciseMatchTransitionMap);
        }
        for (IFATransition transition : batchMatchTransitionList) {
          matchedState = tryGetNextState(child, sourceState, transition);
          if (matchedState == null) {
            continue;
          }
          if (firstMatchedState == null) {
            firstMatchedState = matchedState;
          } else {
            if (matchedStateSet == null) {
              matchedStateSet = new MatchedStateSet(patternFA.getStateSize());
              matchedStateSet.add(firstMatchedState);
              if (firstMatchedState.isFinal()) {
                hasFinalState = true;
              }
            }
            matchedStateSet.add(matchedState);
            if (matchedState.isFinal()) {
              hasFinalState = true;
            }
          }
        }
        if (firstMatchedState == null) {
          continue;
        }

        IStateMatchInfo stateMatchInfo;
        if (matchedStateSet == null) {
          stateMatchInfo = new PreciseStateMatchInfo(firstMatchedState);
        } else {
          stateMatchInfo =
              new BatchStateMatchInfo(
                  matchedStateSet, new UncheckedSourceStateQueue(), hasFinalState);
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

      MatchedStateSet sourceStateSet;
      IFAState sourceState;

      UncheckedSourceStateQueue uncheckedSourceStateQueue;
      MatchedStateSet matchedStateSet;
      IStateMatchInfo stateMatchInfo;
      boolean hasFinalState = false;

      while (iterator.hasNext()) {

        sourceStateSet = sourceStateMatchInfo.getMatchedStateSet();

        uncheckedSourceStateQueue =
            new UncheckedSourceStateQueue(sourceStateMatchInfo.getMatchedStateSet());
        matchedStateSet = new MatchedStateSet(patternFA.getStateSize());

        child = iterator.next();

        for (int i = 0; i < sourceStateSet.end; i++) {
          sourceState = patternFA.getState(sourceStateSet.existingState[i]);
          hasFinalState = checkAllTransitionOfOneSourceState(child, sourceState, matchedStateSet);
          uncheckedSourceStateQueue.remove(sourceState);
          if (!matchedStateSet.isEmpty()) {
            break;
          }
        }

        if (matchedStateSet.isEmpty()) {
          stateMatchInfo = traceback(child);
        } else {
          stateMatchInfo =
              new BatchStateMatchInfo(matchedStateSet, uncheckedSourceStateQueue, hasFinalState);
        }

        if (stateMatchInfo != null) {
          saveResult(child, stateMatchInfo);
          return;
        }
      }
    }
  }

  private boolean checkAllTransitionOfOneSourceState(
      N child, IFAState sourceState, MatchedStateSet matchedStateSet) {
    Map<String, IFATransition> preciseMatchTransitionMap =
        patternFA.getPreciseMatchTransition(sourceState);
    List<IFATransition> batchMatchTransitionList = patternFA.getBatchMatchTransition(sourceState);

    boolean hasFinalState = false;

    IFAState matchedState;
    if (!preciseMatchTransitionMap.isEmpty()) {
      matchedState = tryGetNextState(child, sourceState, preciseMatchTransitionMap);
      if (matchedState != null) {
        matchedStateSet.add(matchedState);
        if (matchedState.isFinal()) {
          hasFinalState = true;
        }
      }
    }
    for (IFATransition transition : batchMatchTransitionList) {
      matchedState = tryGetNextState(child, sourceState, transition);
      if (matchedState != null) {
        matchedStateSet.add(matchedState);
        if (matchedState.isFinal()) {
          hasFinalState = true;
        }
      }
    }
    return hasFinalState;
  }

  private IStateMatchInfo traceback(N node) {
    IStateMatchInfo result = new BatchStateMatchInfo();

    Deque<Integer> uncheckedSourceStateIteratorStack =
        new ArrayDeque<>(ancestorStack.size() - startIndexOfTraceback + 1);
    int uncheckedSourceStateQueueIndex;
    IFAState uncheckedSourceState;

    N currentNode;
    IStateMatchInfo currentStateMatchInfo, childStateMatchInfo;
    MatchedStateSet currentMatchedStateSet;
    int startOfNewMatchedState;
    for (int i = ancestorStack.size() - 1; i >= startIndexOfTraceback; i--) {
      currentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;

      // there's no state not further searched
      if (currentStateMatchInfo.getUncheckedSourceStateQueue().isEmpty()) {
        continue;
      }

      // there's some state not further searched, select them
      uncheckedSourceStateIteratorStack.push(
          currentStateMatchInfo.getUncheckedSourceStateQueue().start);

      // further search the selected state from current ancestor
      int index = i;
      while (!uncheckedSourceStateIteratorStack.isEmpty()) {

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

        uncheckedSourceStateQueueIndex = uncheckedSourceStateIteratorStack.pop();
        if (uncheckedSourceStateQueueIndex
            == currentStateMatchInfo.getUncheckedSourceStateQueue().existingState.length) {
          index--;
          continue;
        }

        uncheckedSourceState =
            patternFA.getState(
                currentStateMatchInfo.getUncheckedSourceStateQueue()
                    .existingState[uncheckedSourceStateQueueIndex++]);
        uncheckedSourceStateIteratorStack.push(uncheckedSourceStateQueueIndex);

        currentMatchedStateSet = currentStateMatchInfo.getMatchedStateSet();
        startOfNewMatchedState = currentMatchedStateSet.end;
        if (checkAllTransitionOfOneSourceState(
            currentNode, uncheckedSourceState, currentMatchedStateSet)) {
          currentStateMatchInfo.setHasFinalState();
        }
        currentStateMatchInfo.removeUncheckedSourceState(uncheckedSourceState);

        if (startOfNewMatchedState == currentMatchedStateSet.end) {
          // no new matched state
          continue;
        }

        if (currentNode == node) {
          return result;
        } else {
          childStateMatchInfo.addUncheckedSourceState(
              startOfNewMatchedState, currentMatchedStateSet);
          uncheckedSourceStateIteratorStack.push(
              childStateMatchInfo.getUncheckedSourceStateQueue().start);
          index++;
        }
      }
    }
    return null;
  }

  protected IFAState tryGetNextState(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    IFATransition transition = preciseMatchTransitionMap.get(node.getName());
    if (transition == null) {
      return null;
    }
    return patternFA.getNextState(sourceState, transition);
  }

  protected IFAState tryGetNextState(N node, IFAState sourceState, IFATransition transition) {
    if (transition.isMatch(node.getName())) {
      return patternFA.getNextState(sourceState, transition);
    } else {
      return null;
    }
  }

  private static class MatchedStateSet {
    private static final int INITIAL_SIZE = 8;

    private final boolean[] stateStatus;

    private int[] existingState = new int[INITIAL_SIZE];

    private int end = 0;

    public MatchedStateSet(int stateSize) {
      stateStatus = new boolean[stateSize];
    }

    public int size() {
      return end;
    }

    public boolean isEmpty() {
      return end == 0;
    }

    public boolean contains(IFAState state) {
      return stateStatus[state.getIndex()];
    }

    public void add(IFAState state) {
      if (stateStatus[state.getIndex()]) {
        return;
      }
      if (end == existingState.length) {
        int[] array = new int[existingState.length * 2];
        System.arraycopy(existingState, 0, array, 0, end);
        existingState = array;
      }
      existingState[end++] = state.getIndex();
      stateStatus[state.getIndex()] = true;
    }
  }

  private static class UncheckedSourceStateQueue {
    private static final int INITIAL_SIZE = 8;

    private int[] existingState;

    private int start;

    public UncheckedSourceStateQueue() {
      existingState = new int[INITIAL_SIZE];
      start = INITIAL_SIZE;
    }

    public UncheckedSourceStateQueue(MatchedStateSet matchedStateSet) {
      existingState = new int[matchedStateSet.existingState.length];
      start = existingState.length - matchedStateSet.end;
      System.arraycopy(matchedStateSet.existingState, 0, existingState, start, matchedStateSet.end);
    }

    public int size() {
      return existingState.length - start;
    }

    public boolean isEmpty() {
      return start == existingState.length;
    }

    public void addAll(int start, MatchedStateSet matchedStateSet) {
      if (!isEmpty()) {
        throw new IllegalStateException();
      }
      int size = matchedStateSet.end - start;
      if (size > existingState.length) {
        int capacity = existingState.length;
        while (capacity < size) {
          capacity *= 2;
        }
        existingState = new int[capacity];
      }

      this.start = existingState.length - size;
      System.arraycopy(matchedStateSet.existingState, start, existingState, this.start, size);
    }

    public void remove(IFAState state) {
      if (state.getIndex() == existingState[start]) {
        start++;
      } else {
        throw new IllegalStateException();
      }
    }
  }
}
