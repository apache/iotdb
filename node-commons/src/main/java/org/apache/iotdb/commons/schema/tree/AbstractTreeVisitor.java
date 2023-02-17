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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.IPatternFA;
import org.apache.iotdb.commons.path.fa.match.IStateMatchInfo;
import org.apache.iotdb.commons.path.fa.match.StateMultiMatchInfo;
import org.apache.iotdb.commons.path.fa.match.StateSingleMatchInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
public abstract class AbstractTreeVisitor<N extends ITreeNode, R>
    implements Iterator<R>, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(AbstractTreeVisitor.class);

  // command parameters
  protected N root;

  // finite automation constructed from given path pattern or pattern tree
  protected final IPatternFA patternFA;

  // run time variables
  // stack to store children iterator of visited ancestor
  private final Deque<VisitorStackEntry> visitorStack = new ArrayDeque<>();
  // stack to store ancestor nodes and their FA state match info
  private final List<AncestorStackEntry> ancestorStack = new ArrayList<>();
  // the FA match process can traceback since this ancestor in ancestor stack
  // this field will be updated during iterating children in all subclass of
  // AbstractChildrenIterator
  private int firstAncestorOfTraceback = -1;
  // the FA state match info of current node
  // this field will be updated during iterating children in all subclass of
  // AbstractChildrenIterator
  private IStateMatchInfo currentStateMatchInfo;
  // whether to visit the subtree of current node
  private boolean shouldVisitSubtree;
  // record exception if failed
  private Throwable throwable;

  // cached result variables
  private N nextMatchedNode;

  // only used for wrapper
  protected AbstractTreeVisitor() {
    root = null;
    patternFA = null;
  }

  protected AbstractTreeVisitor(N root, PartialPath pathPattern, boolean isPrefixMatch) {
    this.root = root;

    boolean usingDFA = false;
    // Use DFA if there are ** and no regex node in pathPattern
    for (String pathNode : pathPattern.getNodes()) {
      if (IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(pathNode)) {
        // ** node
        usingDFA = true;
      } else if (pathNode.length() > 1
          && pathNode.contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
        // regex node
        usingDFA = false;
        break;
      }
    }
    this.patternFA =
        usingDFA
            ? new IPatternFA.Builder().pattern(pathPattern).isPrefixMatch(isPrefixMatch).buildDFA()
            : new IPatternFA.Builder().pattern(pathPattern).isPrefixMatch(isPrefixMatch).buildNFA();
  }

  /** This method must be invoked before iteration */
  protected final void initStack() {
    IFAState initialState = patternFA.getInitialState();
    IFATransition transition =
        patternFA.getPreciseMatchTransition(initialState).get(root.getName());
    if (transition == null) {
      // the visitor stack will be empty and the result of hasNext() will be false
      return;
    }
    IFAState rootState = patternFA.getNextState(initialState, transition);
    currentStateMatchInfo = new StateSingleMatchInfo(patternFA, rootState);
    visitorStack.push(new VisitorStackEntry(createChildrenIterator(root), 1));
    ancestorStack.add(new AncestorStackEntry(root, currentStateMatchInfo));
  }

  public void reset() {
    close();
    visitorStack.clear();
    ancestorStack.clear();
    nextMatchedNode = null;
    firstAncestorOfTraceback = -1;
    initStack();
  }

  @Override
  public void close() {
    if (nextMatchedNode != null && !shouldVisitSubtree) {
      // release nextMatchedNode
      releaseNode(nextMatchedNode);
    }
    while (!visitorStack.isEmpty()) {
      popStack();
    }
  }

  @Override
  public boolean hasNext() {
    if (throwable == null && nextMatchedNode == null) {
      try {
        getNext();
      } catch (Throwable e) {
        logger.warn(e.getMessage(), e);
        setFailure(e);
      }
    }
    return throwable == null && nextMatchedNode != null;
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    R result = generateResult(nextMatchedNode);
    if (!shouldVisitSubtree) {
      // release nextMatchedNode
      releaseNode(nextMatchedNode);
    }
    nextMatchedNode = null;
    return result;
  }

  private void getNext() {
    nextMatchedNode = null;
    VisitorStackEntry stackEntry;
    Iterator<N> iterator;
    while (!visitorStack.isEmpty()) {
      stackEntry = visitorStack.peek();
      iterator = stackEntry.iterator;

      if (!iterator.hasNext()) {
        popStack();
        continue;
      }

      N nextTempNode = iterator.next();

      shouldVisitSubtree = false;
      if (currentStateMatchInfo.hasFinalState()) {
        if (acceptFullMatchedNode(nextTempNode)) {
          nextMatchedNode = nextTempNode;
        }
        shouldVisitSubtree = shouldVisitSubtreeOfFullMatchedNode(nextTempNode);
      } else {
        if (acceptInternalMatchedNode(nextTempNode)) {
          nextMatchedNode = nextTempNode;
        }
        shouldVisitSubtree = shouldVisitSubtreeOfInternalMatchedNode(nextTempNode);
      }

      if (shouldVisitSubtree) {
        pushChildren(nextTempNode);
        // After adding nextTempNode into ancestorStack, nextTempNode will be released finally.
      } else if (nextMatchedNode != nextTempNode) {
        // Else if nextTempNode is not accepted, it needs to be released.
        releaseNode(nextTempNode);
      }
      // Otherwise, it will be released when invoking next()

      if (nextMatchedNode != null) {
        return;
      }
    }
  }

  private void pushChildren(N parent) {
    visitorStack.push(
        new VisitorStackEntry(
            createChildrenIterator(parent),
            visitorStack.isEmpty() ? 1 : visitorStack.peek().level + 1));
    ancestorStack.add(new AncestorStackEntry(parent, currentStateMatchInfo));
  }

  private AbstractChildrenIterator createChildrenIterator(N parent) {
    if (firstAncestorOfTraceback > -1) {
      // there may be traceback when try to find the matched state of node
      return new TraceBackChildrenIterator(parent, currentStateMatchInfo);
    } else if (currentStateMatchInfo.hasOnlyPreciseMatchTransition()) {
      // the child can be got directly with the precise value of transition
      return new PreciseMatchChildrenIterator(parent, currentStateMatchInfo.getOneMatchedState());
    } else if (currentStateMatchInfo.hasNoPreciseMatchTransition()
        && currentStateMatchInfo.isSingleFuzzyMatchTransition()) {
      // only one transition which may match batch children, need to iterate and check all child
      return new SingleFuzzyMatchChildrenIterator(
          parent, currentStateMatchInfo.getOneMatchedState());
    } else {
      // child may be matched by multi transitions, precise match or fuzzy match,
      // which results in one child match multi state; need to iterate and check all child
      return new MultiMatchTransitionChildrenIterator(
          parent, currentStateMatchInfo.getOneMatchedState());
    }
  }

  private void popStack() {
    VisitorStackEntry stackEntry = visitorStack.pop();
    stackEntry.iterator.close();
    // The ancestor pop operation with level check supports the children of one node pushed by
    // batch.
    if (!visitorStack.isEmpty() && visitorStack.peek().level < ancestorStack.size()) {
      AncestorStackEntry ancestorStackEntry = ancestorStack.remove(ancestorStack.size() - 1);
      releaseNode(ancestorStackEntry.node);
      if (ancestorStack.size() <= firstAncestorOfTraceback) {
        firstAncestorOfTraceback = -1;
      }
    }
  }

  /**
   * Get full path of parent of current node. This method should be used in {@linkplain
   * AbstractTreeVisitor#acceptInternalMatchedNode}, {@linkplain
   * AbstractTreeVisitor#acceptFullMatchedNode},{@linkplain
   * AbstractTreeVisitor#shouldVisitSubtreeOfInternalMatchedNode} or {@linkplain
   * AbstractTreeVisitor#shouldVisitSubtreeOfFullMatchedNode}.
   *
   * @return full path from traverse start node to the parent of current node
   */
  protected PartialPath getParentPartialPath() {
    List<String> nodeNames = new ArrayList<>();
    Iterator<AncestorStackEntry> iterator = ancestorStack.iterator();
    for (int i = 0, size = shouldVisitSubtree ? ancestorStack.size() - 1 : ancestorStack.size();
        i < size;
        i++) {
      if (iterator.hasNext()) {
        nodeNames.add(iterator.next().node.getName());
      }
    }
    return new PartialPath(nodeNames.toArray(new String[0]));
  }

  /**
   * Get partial path from root to node.
   *
   * @param node node must be concluded in ancestorStack or nextMatchedNode
   * @return partial path from traverse start node to the specified node
   */
  protected final PartialPath getPartialPathFromRootToNode(N node) {
    return new PartialPath(getFullPathFromRootToNode(node));
  }

  /**
   * Get full path from root to node.
   *
   * @param node node must be concluded in ancestorStack or nextMatchedNode
   * @return full path from traverse start node to the specified node
   */
  protected final String[] getFullPathFromRootToNode(N node) {
    List<String> nodeNames = new ArrayList<>();
    for (AncestorStackEntry entry : ancestorStack) {
      nodeNames.add(entry.node.getName());
      if (entry.node == node) {
        return nodeNames.toArray(new String[0]);
      }
    }
    nodeNames.add(node.getName());
    return nodeNames.toArray(new String[0]);
  }

  protected final N getAncestorNodeByLevel(int level) {
    return ancestorStack.get(level).node;
  }

  protected final N getParentOfNextMatchedNode() {
    if (shouldVisitSubtree) {
      return ancestorStack.get(ancestorStack.size() - 2).node;
    } else {
      return ancestorStack.get(ancestorStack.size() - 1).node;
    }
  }

  /**
   * Get level from root to NextMatchedNode. Level of root is 0. For example, root.sg.d1.s1,
   * NextMatchedNode is s1, then return 3.
   *
   * @return level from root to NextMatchedNode
   */
  protected final int getLevelOfNextMatchedNode() {
    if (shouldVisitSubtree) {
      return ancestorStack.size() - 1;
    } else {
      return ancestorStack.size();
    }
  }

  protected final int getSizeOfAncestor() {
    return ancestorStack.size();
  }

  protected void setFailure(Throwable e) {
    this.throwable = e;
  }

  public Throwable getFailure() {
    return throwable;
  }

  public boolean isSuccess() {
    return throwable == null;
  }

  // Get a child with the given childName.
  protected abstract N getChild(N parent, String childName) throws Exception;

  // Get an iterator of all children.
  protected abstract Iterator<N> getChildrenIterator(N parent) throws Exception;

  // Release a child node.
  protected void releaseNode(N node) {}

  // Release an iterator. It is not necessary to deal with all the elements in the iterator.
  // Only the elements that have been fetched but not returned by next() need to be released.
  protected void releaseNodeIterator(Iterator<N> nodeIterator) {}

  /**
   * Internal-match means the node matches an internal node name of the given path pattern. root.sg
   * internal match root.sg.**(pattern). This method should be implemented according to concrete
   * tasks.
   *
   * <p>Return whether the subtree of given node should be processed. If return true, the traversing
   * process will keep traversing the subtree. If return false, the traversing process will skip the
   * subtree of given node.
   */
  protected abstract boolean shouldVisitSubtreeOfInternalMatchedNode(N node);

  /**
   * Full-match means the node matches the last node name of the given path pattern. root.sg.d full
   * match root.sg.**(pattern) This method should be implemented according to concrete tasks.
   *
   * <p>Return whether the subtree of given node should be processed. If return true, the traversing
   * process will keep traversing the subtree. If return false, the traversing process will skip the
   * subtree of given node.
   */
  protected abstract boolean shouldVisitSubtreeOfFullMatchedNode(N node);

  /** Only accepted nodes will be considered for hasNext() and next() */
  protected abstract boolean acceptInternalMatchedNode(N node);

  /** Only accepted nodes will be considered for hasNext() and next() */
  protected abstract boolean acceptFullMatchedNode(N node);

  /** The method used for generating the result based on the matched node. */
  protected abstract R generateResult(N nextMatchedNode);

  private class VisitorStackEntry {

    // children iterator
    private final AbstractChildrenIterator iterator;

    // level of children taken from iterator, start from 1
    private final int level;

    VisitorStackEntry(AbstractChildrenIterator iterator, int level) {
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

  // implement common iterating logic of different children iterator
  private abstract class AbstractChildrenIterator implements Iterator<N> {

    private N nextMatchedChild;

    @Override
    public boolean hasNext() {
      if (nextMatchedChild == null) {
        try {
          getNext();
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
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

    protected final void saveResult(N child, IStateMatchInfo stateMatchInfo) {
      nextMatchedChild = child;
      currentStateMatchInfo = stateMatchInfo;
    }

    protected abstract void getNext() throws Exception;

    protected void close() {
      if (nextMatchedChild != null) {
        releaseNode(nextMatchedChild);
      }
    }
  }

  // the child can be got directly with the precise value of transition, there's no traceback
  private class PreciseMatchChildrenIterator extends AbstractChildrenIterator {
    private final N parent;
    private final IFAState sourceState;
    private final Iterator<IFATransition> transitionIterator;

    private PreciseMatchChildrenIterator(N parent, IFAState sourceState) {
      this.parent = parent;
      this.sourceState = sourceState;
      transitionIterator = patternFA.getPreciseMatchTransitionIterator(sourceState);
    }

    @Override
    protected void getNext() throws Exception {
      IFATransition transition;
      while (transitionIterator.hasNext()) {
        transition = transitionIterator.next();
        N child = getChild(parent, transition.getAcceptEvent());
        if (child == null) {
          continue;
        }
        saveResult(
            child,
            new StateSingleMatchInfo(patternFA, patternFA.getNextState(sourceState, transition)));
        return;
      }
    }
  }

  // only one fuzzy transition which may match batch children, need to iterate and check all
  // children,
  // there's no traceback
  private class SingleFuzzyMatchChildrenIterator extends AbstractChildrenIterator {

    private final IFAState sourceState;
    private final IFATransition transition;
    private final StateSingleMatchInfo stateMatchInfo;
    private final N parent;

    private Iterator<N> childrenIterator;

    private SingleFuzzyMatchChildrenIterator(N parent, IFAState sourceState) {
      this.sourceState = sourceState;
      this.transition = patternFA.getFuzzyMatchTransitionIterator(sourceState).next();
      this.stateMatchInfo =
          new StateSingleMatchInfo(patternFA, patternFA.getNextState(sourceState, transition));
      this.parent = parent;
    }

    @Override
    protected void getNext() throws Exception {
      if (childrenIterator == null) {
        this.childrenIterator = getChildrenIterator(parent);
      }
      N child;
      while (childrenIterator.hasNext()) {
        child = childrenIterator.next();
        if (tryGetNextState(child, sourceState, transition) == null) {
          releaseNode(child);
          continue;
        }
        saveResult(child, stateMatchInfo);
        return;
      }
    }

    @Override
    protected void close() {
      super.close();
      if (childrenIterator != null) {
        releaseNodeIterator(childrenIterator);
      }
    }
  }

  // child may be matched by multi transitions, precise match or fuzzy match,
  // which results in one child match multi state; need to iterate and check all child.
  // the iterating process will try to get the first matched state of a child, and if there are some
  // rest transitions, there may be traceback when checking the descendents
  private class MultiMatchTransitionChildrenIterator extends AbstractChildrenIterator {

    private final IFAState sourceState;
    private final Map<String, IFATransition> preciseMatchTransitionMap;
    private final N parent;

    private Iterator<N> iterator;

    private MultiMatchTransitionChildrenIterator(N parent, IFAState sourceState) {
      this.sourceState = sourceState;
      this.preciseMatchTransitionMap = patternFA.getPreciseMatchTransition(sourceState);
      this.parent = parent;
    }

    @Override
    protected void getNext() throws Exception {
      if (iterator == null) {
        this.iterator = getChildrenIterator(parent);
      }
      N child;

      IFAState matchedState = null;
      Iterator<IFATransition> transitionIterator;
      IStateMatchInfo stateMatchInfo;
      while (iterator.hasNext()) {
        child = iterator.next();

        if (!preciseMatchTransitionMap.isEmpty()) {
          matchedState = tryGetNextState(child, sourceState, preciseMatchTransitionMap);
        }

        transitionIterator = patternFA.getFuzzyMatchTransitionIterator(sourceState);
        if (matchedState == null) {
          while (transitionIterator.hasNext()) {
            matchedState = tryGetNextState(child, sourceState, transitionIterator.next());
            if (matchedState != null) {
              break;
            }
          }
          if (matchedState == null) {
            releaseNode(child);
            continue;
          }
        }

        if (patternFA.mayTransitionOverlap()) {
          if (transitionIterator.hasNext()) {
            stateMatchInfo = new StateMultiMatchInfo(patternFA, matchedState, transitionIterator);
            firstAncestorOfTraceback = ancestorStack.size();
          } else {
            stateMatchInfo = new StateSingleMatchInfo(patternFA, matchedState);
          }
        } else {
          stateMatchInfo = new StateSingleMatchInfo(patternFA, matchedState);
        }
        saveResult(child, stateMatchInfo);
        return;
      }
    }

    @Override
    protected void close() {
      super.close();
      if (iterator != null) {
        releaseNodeIterator(iterator);
      }
    }
  }

  // there may be traceback when try to find the matched state of node;
  // the iterating process will try to get the first matched state of a child.
  private class TraceBackChildrenIterator extends AbstractChildrenIterator {

    private final N parent;
    private final IStateMatchInfo sourceStateMatchInfo;

    private Iterator<N> iterator;

    TraceBackChildrenIterator(N parent, IStateMatchInfo sourceStateMatchInfo) {
      this.sourceStateMatchInfo = sourceStateMatchInfo;
      this.parent = parent;
    }

    @Override
    protected void getNext() throws Exception {
      if (iterator == null) {
        iterator = getChildrenIterator(parent);
      }
      N child;

      IFAState sourceState;

      IStateMatchInfo stateMatchInfo;
      Iterator<IFATransition> transitionIterator;

      while (iterator.hasNext()) {

        child = iterator.next();

        stateMatchInfo = new StateMultiMatchInfo(patternFA);
        for (int i = 0; i < sourceStateMatchInfo.getMatchedStateSize(); i++) {
          sourceState = sourceStateMatchInfo.getMatchedState(i);
          transitionIterator = tryGetNextMatchedState(child, sourceState, stateMatchInfo);
          if (stateMatchInfo.getMatchedStateSize() > 0) {
            stateMatchInfo.setSourceStateOrdinal(i);
            stateMatchInfo.setSourceTransitionIterator(transitionIterator);
            break;
          }
        }

        if (stateMatchInfo.getMatchedStateSize() == 0) {
          traceback(child, stateMatchInfo, sourceStateMatchInfo.getMatchedStateSize() - 1);
          if (stateMatchInfo.getMatchedStateSize() == 0) {
            releaseNode(child);
            continue;
          }
        }

        saveResult(child, stateMatchInfo);
        return;
      }
    }

    /**
     * Try to get next matched state from sourceState and add it into currentStateMatchInfo
     *
     * @param child child node to match
     * @param sourceState source state
     * @param currentStateMatchInfo currentStateMatchInfo
     * @return iterator of rest transitions
     */
    private Iterator<IFATransition> tryGetNextMatchedState(
        N child, IFAState sourceState, IStateMatchInfo currentStateMatchInfo) {
      Map<String, IFATransition> preciseMatchTransitionMap =
          patternFA.getPreciseMatchTransition(sourceState);

      IFAState matchedState;
      if (!preciseMatchTransitionMap.isEmpty()) {
        matchedState = tryGetNextState(child, sourceState, preciseMatchTransitionMap);
        if (matchedState != null) {
          currentStateMatchInfo.addMatchedState(matchedState);
          return patternFA.getFuzzyMatchTransitionIterator(sourceState);
        }
      }

      Iterator<IFATransition> transitionIterator =
          patternFA.getFuzzyMatchTransitionIterator(sourceState);
      while (transitionIterator.hasNext()) {
        matchedState = tryGetNextState(child, sourceState, transitionIterator.next());
        if (matchedState != null) {
          currentStateMatchInfo.addMatchedState(matchedState);
          return transitionIterator;
        }
      }
      return transitionIterator;
    }

    private void traceback(N node, IStateMatchInfo stateMatchInfo, int checkedSourceStateOrdinal) {
      IStateMatchInfo parentStateMatchInfo;

      N currentNode;
      IStateMatchInfo currentStateMatchInfo;

      int sourceStateOrdinal;
      IFAState sourceState;
      Iterator<IFATransition> transitionIterator = null;

      int matchedStateSize;
      IFAState matchedState;

      int currentNodeIndex;
      for (int i = ancestorStack.size() - 1; i >= firstAncestorOfTraceback; i--) {
        parentStateMatchInfo = ancestorStack.get(i - 1).stateMatchInfo;
        currentStateMatchInfo = ancestorStack.get(i).stateMatchInfo;

        // there's no state not further searched
        if (currentStateMatchInfo.getSourceStateOrdinal()
            == parentStateMatchInfo.getMatchedStateSize()) {
          continue;
        }

        // there's some state not further searched, process them in order
        currentNodeIndex = i;
        while (currentNodeIndex >= i) {
          parentStateMatchInfo = ancestorStack.get(currentNodeIndex - 1).stateMatchInfo;

          if (currentNodeIndex == ancestorStack.size()) {
            currentNode = node;
            currentStateMatchInfo = stateMatchInfo;
          } else {
            currentNode = ancestorStack.get(currentNodeIndex).node;
            currentStateMatchInfo = ancestorStack.get(currentNodeIndex).stateMatchInfo;
          }

          matchedState = null;
          if (currentNode == node) {
            sourceStateOrdinal = checkedSourceStateOrdinal;
          } else {
            sourceStateOrdinal = currentStateMatchInfo.getSourceStateOrdinal();
            if (sourceStateOrdinal == parentStateMatchInfo.getMatchedStateSize()) {
              currentNodeIndex--;
              continue;
            }
            // there may be some states could be matched from transition of current source state
            sourceState = parentStateMatchInfo.getMatchedState(sourceStateOrdinal);
            transitionIterator = currentStateMatchInfo.getSourceTransitionIterator();
            while (transitionIterator.hasNext()) {
              matchedState = tryGetNextState(currentNode, sourceState, transitionIterator.next());
              if (matchedState != null) {
                break;
              }
            }
          }

          if (matchedState == null) {
            while (++sourceStateOrdinal < parentStateMatchInfo.getMatchedStateSize()) {
              sourceState = parentStateMatchInfo.getMatchedState(sourceStateOrdinal);
              matchedStateSize = currentStateMatchInfo.getMatchedStateSize();
              transitionIterator =
                  tryGetNextMatchedState(currentNode, sourceState, currentStateMatchInfo);
              // change of matchedStateSize means currentNode there is transition from sourceState
              // matching currentNode
              if (matchedStateSize != currentStateMatchInfo.getMatchedStateSize()) {
                matchedState = currentStateMatchInfo.getMatchedState(matchedStateSize);
                currentStateMatchInfo.setSourceStateOrdinal(sourceStateOrdinal);
                currentStateMatchInfo.setSourceTransitionIterator(transitionIterator);
                break;
              }
            }
            if (matchedState == null) {
              currentStateMatchInfo.setSourceStateOrdinal(sourceStateOrdinal - 1);
              currentStateMatchInfo.setSourceTransitionIterator(transitionIterator);
              currentNodeIndex--;
              continue;
            }
          }

          currentStateMatchInfo.addMatchedState(matchedState);

          if (currentNode == node) {
            return;
          } else {
            currentNodeIndex++;
          }
        }
      }
    }

    @Override
    protected void close() {
      super.close();
      if (iterator != null) {
        releaseNodeIterator(iterator);
      }
    }
  }

  // the match process of FA graph is a dfs on FA Graph

  // a tmp way to process alias of measurement node, which may results in multi event when checking
  // the transition;
  // fortunately, the measurement node only match the final state, which means there won't be any
  // multi transition and traceback judge
  protected IFAState tryGetNextState(
      N node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    IFATransition transition = preciseMatchTransitionMap.get(node.getName());
    if (transition == null) {
      return null;
    }
    return patternFA.getNextState(sourceState, transition);
  }

  // a tmp way to process alias of measurement node, which may results in multi event when checking
  // the transition;
  // fortunately, the measurement node only match the final state, which means there won't be any
  // multi transition and traceback judge
  protected IFAState tryGetNextState(N node, IFAState sourceState, IFATransition transition) {
    if (transition.isMatch(node.getName())) {
      return patternFA.getNextState(sourceState, transition);
    } else {
      return null;
    }
  }
}
