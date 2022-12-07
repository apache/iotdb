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

  public void reset() {
    visitorStack.clear();
    ancestorStack.clear();
    nextMatchedNode = null;
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
    visitorStack.push(
        new VisitorStackEntry(
            new ChildrenIterator(parent, currentStateMatchInfo), visitorStack.peek().level + 1));
    ancestorStack.add(new AncestorStackEntry(parent, currentStateMatchInfo));
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

  protected IFATransition getMatchedTransition(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    return preciseMatchTransitionMap.get(node.getName());
  }

  protected boolean checkIsMatch(N node, IFAState sourceState, IFATransition transition) {
    return transition.isMatch(node.getName());
  }

  private StateMatchInfo traceback(N node, StateMatchInfo currentStateMatchInfo) {
    StateMatchInfo result = new StateMatchInfo();
    result.checkedSourceStateSet.addAll(currentStateMatchInfo.matchedStateSet);
    N childNode;
    StateMatchInfo parentStateMatchInfo = result, childStateMatchInfo;
    Set<IFAState> addedState;
    for (int i = ancestorStack.size() - 1, end = currentStateMatchInfo.indexOfTraceback;
        i >= end;
        i--) {
      childStateMatchInfo = parentStateMatchInfo;
      parentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;
      addedState = new HashSet<>();
      for (IFAState sourceState : parentStateMatchInfo.matchedStateSet) {
        if (!childStateMatchInfo.checkedSourceStateSet.contains(sourceState)) {
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
        for (IFATransition transition : patternFA.getTransition(sourceState)) {
          if (checkIsMatch(childNode, sourceState, transition)) {
            matchedState = patternFA.getNextState(sourceState, transition);
            if (childStateMatchInfo.matchedStateSet.contains(matchedState)) {
              continue;
            }
            addedState.add(matchedState);
            childStateMatchInfo.addState(matchedState);
          }
        }
        childStateMatchInfo.checkedSourceStateSet.add(sourceState);
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

  private class ChildrenIterator implements Iterator<N> {

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
          for (String childName : patternFA.getPreciseMatchTransition(sourceState).keySet()) {
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
      Set<IFAState> checkedSourceStateSet = new HashSet<>();
      Set<IFAState> matchedStateSet = new HashSet<>();

      while (iterator.hasNext()) {
        child = iterator.next();
        for (IFAState sourceState : sourceStateMatchInfo.matchedStateSet) {
          preciseMatchTransitionMap = patternFA.getPreciseMatchTransition(sourceState);
          batchMatchTransitionList = patternFA.getBatchMatchTransition(sourceState);

          if (!preciseMatchTransitionMap.isEmpty()) {
            targetTransition = getMatchedTransition(child, sourceState, preciseMatchTransitionMap);
            if (targetTransition != null) {
              hasResult = true;
              matchedState = patternFA.getNextState(sourceState, targetTransition);
              matchedStateSet.add(matchedState);
            }
            if (hasResult) {
              for (IFATransition transition : preciseMatchTransitionMap.values()) {
                if (transition.equals(targetTransition)) {
                  continue;
                }
                if (checkIsMatch(child, sourceState, transition)) {
                  matchedState = patternFA.getNextState(sourceState, transition);
                  matchedStateSet.add(matchedState);
                }
              }
            }
          }
          for (IFATransition transition : batchMatchTransitionList) {
            if (checkIsMatch(child, sourceState, transition)) {
              hasResult = true;
              matchedState = patternFA.getNextState(sourceState, transition);
              matchedStateSet.add(matchedState);
            }
          }
          checkedSourceStateSet.add(sourceState);
          if (hasResult) {
            break;
          }
        }

        if (hasResult) {
          currentStateMatchInfo = new StateMatchInfo(matchedStateSet, checkedSourceStateSet);
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

  private class VisitorStackEntry {

    private final ChildrenIterator iterator;

    private final int level;

    VisitorStackEntry(ChildrenIterator iterator, int level) {
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

  private class StateMatchInfo {

    private final Set<IFAState> matchedStateSet;

    /** SourceState, matched by parent */
    private final Set<IFAState> checkedSourceStateSet;

    private boolean hasBatchMatchTransition = false;

    private boolean hasFinalState = false;

    private int indexOfTraceback = -1;

    StateMatchInfo() {
      matchedStateSet = new HashSet<>();
      checkedSourceStateSet = new HashSet<>();
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

    private void addState(IFAState state) {
      matchedStateSet.add(state);
      if (state.isFinal()) {
        hasFinalState = true;
      }
      if (patternFA.getBatchMatchTransition(state).size() > 0) {
        hasBatchMatchTransition = true;
      }
    }
  }
}
