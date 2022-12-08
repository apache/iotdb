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
      if (ancestorStack.size() <= startIndexOfTraceback) {
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

    void setHasFinalState();

    boolean hasOnlyPreciseMatchTransition();

    boolean hasNoPreciseMatchTransition();

    boolean isSingleBatchMatchTransition();

    IFAState getOneMatchedState();

    void addMatchedState(IFAState state);

    IFAState getMatchedState(int index);

    int getMatchedStateSize();

    int getSourceStateIndex();

    void setSourceStateIndex(int sourceStateIndex);

    Iterator<IFATransition> getSourceStateTransitionIterator();

    void setSourceStateTransitionIterator(Iterator<IFATransition> sourceStateTransitionIterator);
  }

  private class BatchStateMatchInfo implements IStateMatchInfo {

    private final MatchedStateSet matchedStateSet;

    private int sourceStateIndex;

    private Iterator<IFATransition> sourceStateTransitionIterator;

    private boolean hasFinalState = false;

    BatchStateMatchInfo() {
      matchedStateSet = new MatchedStateSet(patternFA.getStateSize());
    }

    BatchStateMatchInfo(
        IFAState matchedState, Iterator<IFATransition> sourceStateTransitionIterator) {
      matchedStateSet = new MatchedStateSet(patternFA.getStateSize());
      matchedStateSet.add(matchedState);
      sourceStateIndex = 0;
      this.sourceStateTransitionIterator = sourceStateTransitionIterator;
      this.hasFinalState = matchedState.isFinal();
    }

    @Override
    public boolean hasFinalState() {
      return hasFinalState;
    }

    @Override
    public void setHasFinalState() {
      hasFinalState = true;
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
    public void addMatchedState(IFAState state) {
      matchedStateSet.add(state);
      if (state.isFinal()) {
        hasFinalState = true;
      }
    }

    @Override
    public IFAState getMatchedState(int index) {
      return patternFA.getState(matchedStateSet.existingState[index]);
    }

    @Override
    public int getMatchedStateSize() {
      return matchedStateSet.end;
    }

    @Override
    public int getSourceStateIndex() {
      return sourceStateIndex;
    }

    @Override
    public void setSourceStateIndex(int sourceStateIndex) {
      this.sourceStateIndex = sourceStateIndex;
    }

    @Override
    public Iterator<IFATransition> getSourceStateTransitionIterator() {
      return sourceStateTransitionIterator;
    }

    @Override
    public void setSourceStateTransitionIterator(
        Iterator<IFATransition> sourceStateTransitionIterator) {
      this.sourceStateTransitionIterator = sourceStateTransitionIterator;
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
    public void setHasFinalState() {
      throw new UnsupportedOperationException();
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
    public void addMatchedState(IFAState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IFAState getMatchedState(int index) {
      if (index == 0) {
        return matchedState;
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public int getMatchedStateSize() {
      return 1;
    }

    @Override
    public int getSourceStateIndex() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setSourceStateIndex(int sourceStateIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<IFATransition> getSourceStateTransitionIterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setSourceStateTransitionIterator(
        Iterator<IFATransition> sourceStateTransitionIterator) {
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

      IFAState matchedState = null;
      Iterator<IFATransition> transitionIterator;
      IStateMatchInfo stateMatchInfo;
      while (iterator.hasNext()) {
        child = iterator.next();

        if (!preciseMatchTransitionMap.isEmpty()) {
          matchedState = tryGetNextState(child, sourceState, preciseMatchTransitionMap);
        }

        transitionIterator = batchMatchTransitionList.iterator();
        if (matchedState == null) {
          while (transitionIterator.hasNext()) {
            matchedState = tryGetNextState(child, sourceState, transitionIterator.next());
            if (matchedState != null) {
              break;
            }
          }
          if (matchedState == null) {
            continue;
          }
        }

        if (transitionIterator.hasNext()) {
          stateMatchInfo = new BatchStateMatchInfo(matchedState, transitionIterator);
          startIndexOfTraceback = ancestorStack.size();
        } else {
          stateMatchInfo = new PreciseStateMatchInfo(matchedState);
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

      IFAState sourceState;

      IStateMatchInfo stateMatchInfo;
      Iterator<IFATransition> transitionIterator;

      while (iterator.hasNext()) {

        child = iterator.next();

        stateMatchInfo = new BatchStateMatchInfo();
        for (int i = 0; i < sourceStateMatchInfo.getMatchedStateSize(); i++) {
          sourceState = sourceStateMatchInfo.getMatchedState(i);
          transitionIterator = tryGetOneMatchedState(child, sourceState, stateMatchInfo);
          if (stateMatchInfo.getMatchedStateSize() > 0) {
            stateMatchInfo.setSourceStateIndex(i);
            stateMatchInfo.setSourceStateTransitionIterator(transitionIterator);
            break;
          }
        }

        if (stateMatchInfo.getMatchedStateSize() == 0) {
          traceback(child, stateMatchInfo);
          if (stateMatchInfo.getMatchedStateSize() == 0) {
            continue;
          }
        }

        saveResult(child, stateMatchInfo);
        return;
      }
    }
  }

  private Iterator<IFATransition> tryGetOneMatchedState(
      N child, IFAState sourceState, IStateMatchInfo currentStateMatchInfo) {
    Map<String, IFATransition> preciseMatchTransitionMap =
        patternFA.getPreciseMatchTransition(sourceState);

    IFAState matchedState;
    if (!preciseMatchTransitionMap.isEmpty()) {
      matchedState = tryGetNextState(child, sourceState, preciseMatchTransitionMap);
      if (matchedState != null) {
        currentStateMatchInfo.addMatchedState(matchedState);
        return patternFA.getBatchMatchTransition(sourceState).iterator();
      }
    }

    Iterator<IFATransition> transitionIterator =
        patternFA.getBatchMatchTransition(sourceState).iterator();
    while (transitionIterator.hasNext()) {
      matchedState = tryGetNextState(child, sourceState, transitionIterator.next());
      if (matchedState != null) {
        currentStateMatchInfo.addMatchedState(matchedState);
        return transitionIterator;
      }
    }
    return transitionIterator;
  }

  private void traceback(N node, IStateMatchInfo stateMatchInfo) {
    IStateMatchInfo parentStateMatchInfo;

    N currentNode;
    IStateMatchInfo currentStateMatchInfo;

    int sourceStateIndex;
    IFAState sourceState;
    Iterator<IFATransition> transitionIterator = null;

    int matchedStateSize;
    IFAState matchedState;

    int index;
    for (int i = ancestorStack.size() - 1; i >= startIndexOfTraceback; i--) {
      parentStateMatchInfo = ancestorStack.get(i - 1).stateMatchInfo;
      currentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;

      // there's no state not further searched
      if (currentStateMatchInfo.getSourceStateIndex()
          == parentStateMatchInfo.getMatchedStateSize()) {
        continue;
      }

      // there's some state not further searched, select them
      index = i;
      while (index >= i) {
        parentStateMatchInfo = ancestorStack.get(index - 1).stateMatchInfo;

        if (index == ancestorStack.size()) {
          currentNode = node;
          currentStateMatchInfo = stateMatchInfo;
        } else {
          currentNode = ancestorStack.get(index).node;
          currentStateMatchInfo = ancestorStack.get(index).stateMatchInfo;
        }

        matchedState = null;
        if (currentNode == node) {
          sourceStateIndex = -1;
        } else {
          sourceStateIndex = currentStateMatchInfo.getSourceStateIndex();
          if (sourceStateIndex == parentStateMatchInfo.getMatchedStateSize()) {
            index--;
            continue;
          }
          sourceState = parentStateMatchInfo.getMatchedState(sourceStateIndex);
          transitionIterator = currentStateMatchInfo.getSourceStateTransitionIterator();
          while (transitionIterator.hasNext()) {
            matchedState = tryGetNextState(currentNode, sourceState, transitionIterator.next());
            if (matchedState != null) {
              break;
            }
          }
        }

        if (matchedState == null) {
          while (++sourceStateIndex < parentStateMatchInfo.getMatchedStateSize()) {
            sourceState = parentStateMatchInfo.getMatchedState(sourceStateIndex);
            matchedStateSize = currentStateMatchInfo.getMatchedStateSize();
            transitionIterator =
                tryGetOneMatchedState(currentNode, sourceState, currentStateMatchInfo);
            if (matchedStateSize != currentStateMatchInfo.getMatchedStateSize()) {
              matchedState = currentStateMatchInfo.getMatchedState(matchedStateSize);
              currentStateMatchInfo.setSourceStateIndex(sourceStateIndex);
              currentStateMatchInfo.setSourceStateTransitionIterator(transitionIterator);
              break;
            }
          }
          if (matchedState == null) {
            currentStateMatchInfo.setSourceStateIndex(sourceStateIndex - 1);
            currentStateMatchInfo.setSourceStateTransitionIterator(transitionIterator);
            index--;
            continue;
          }
        }

        currentStateMatchInfo.addMatchedState(matchedState);

        if (currentNode == node) {
          return;
        } else {
          index++;
        }
      }
    }
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
}
