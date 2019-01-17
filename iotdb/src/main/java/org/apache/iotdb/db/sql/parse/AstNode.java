/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.sql.parse;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;

public class AstNode extends CommonTree implements Node, Serializable {

  private static final long serialVersionUID = 1L;
  private transient StringBuilder astStr;
  private transient AstNodeOrigin origin;
  private transient int startIndx = -1;
  private transient int endIndx = -1;
  private transient AstNode rootNode;
  private transient boolean isValidAstStr;
  private transient boolean visited = false;

  public AstNode() {
  }

  /**
   * Constructor.
   *
   * @param t Token for the CommonTree Node
   */
  public AstNode(Token t) {
    super(t);
  }

  public AstNode(AstNode node) {
    super(node);
    this.origin = node.origin;
  }

  @Override
  public Tree dupNode() {
    return new AstNode(this);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.lib.Node#getChildren()
   */
  @Override
  public ArrayList<Node> getChildren() {
    if (super.getChildCount() == 0) {
      return null;
    }

    ArrayList<Node> retVec = new ArrayList<Node>();
    for (int i = 0; i < super.getChildCount(); ++i) {
      retVec.add((Node) super.getChild(i));
    }

    return retVec;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.lib.Node#getName()
   */
  @Override
  public String getName() {
    return String.valueOf(super.getToken().getType());
  }

  /**
   * For every node in this subtree, make sure it's start/stop token's are set. Walk depth first,
   * visit bottom up. Only updates nodes with at least one token index < 0.
   * <p> </p>
   * In contrast to the method in the parent class, this method is iterative.
   */
  @Override
  public void setUnknownTokenBoundaries() {
    Deque<AstNode> stack1 = new ArrayDeque<AstNode>();
    Deque<AstNode> stack2 = new ArrayDeque<AstNode>();
    stack1.push(this);

    while (!stack1.isEmpty()) {
      AstNode next = stack1.pop();
      stack2.push(next);

      if (next.children != null) {
        for (int i = next.children.size() - 1; i >= 0; i--) {
          stack1.push((AstNode) next.children.get(i));
        }
      }
    }

    while (!stack2.isEmpty()) {
      AstNode next = stack2.pop();

      if (next.children == null) {
        if (next.startIndex < 0 || next.stopIndex < 0) {
          next.startIndex = next.stopIndex = next.token.getTokenIndex();
        }
      } else if (next.startIndex >= 0 && next.stopIndex >= 0) {
        continue;
      } else if (next.children.size() > 0) {
        AstNode firstChild = (AstNode) next.children.get(0);
        AstNode lastChild = (AstNode) next.children.get(next.children.size() - 1);
        next.startIndex = firstChild.getTokenStartIndex();
        next.stopIndex = lastChild.getTokenStopIndex();
      }
    }
  }

  /**
   * Get origin AstNodeOrigin.
   *
   * @return information about the object from which this AstNode originated, or null if this
   *     AstNode was not expanded from an object reference
   */
  public AstNodeOrigin getOrigin() {
    return origin;
  }

  /**
   * Tag this AstNode with information about the object from which this node originated.
   */
  public void setOrigin(AstNodeOrigin origin) {
    this.origin = origin;
  }

  /**
   * dump string.
   *
   * @return dump string
   */
  public String dump() {
    StringBuilder sb = new StringBuilder("\n");
    dump(sb);
    return sb.toString();
  }

  private StringBuilder dump(StringBuilder sb) {
    Deque<AstNode> stack = new ArrayDeque<AstNode>();
    stack.push(this);
    int tabLength = 0;

    while (!stack.isEmpty()) {
      AstNode next = stack.peek();

      if (!next.visited) {
        sb.append(StringUtils.repeat(" ", tabLength * 3));
        sb.append(next.toString());
        sb.append("\n");

        if (next.children != null) {
          for (int i = next.children.size() - 1; i >= 0; i--) {
            stack.push((AstNode) next.children.get(i));
          }
        }

        tabLength++;
        next.visited = true;
      } else {
        tabLength--;
        next.visited = false;
        stack.pop();
      }
    }

    return sb;
  }

  private void getRootNodeWithValidAstStr() {

    if (rootNode != null && rootNode.parent == null && rootNode.hasValidMemoizedString()) {
      return;
    }
    AstNode retNode = this;
    while (retNode.parent != null) {
      retNode = (AstNode) retNode.parent;
    }
    rootNode = retNode;
    if (!rootNode.isValidAstStr) {
      rootNode.astStr = new StringBuilder();
      rootNode.toStringTree(rootNode);
      rootNode.isValidAstStr = true;
    }
  }

  private boolean hasValidMemoizedString() {
    return isValidAstStr && astStr != null;
  }

  private void resetRootInformation() {
    // Reset the previously stored rootNode string
    if (rootNode != null) {
      rootNode.astStr = null;
      rootNode.isValidAstStr = false;
    }
  }

  private int getMemoizedStringLen() {
    return astStr == null ? 0 : astStr.length();
  }

  private String getMemoizedSubString(int start, int end) {
    return (astStr == null || start < 0 || end > astStr.length() || start >= end) ? null
        : astStr.subSequence(start, end).toString();
  }

  private void addtoMemoizedString(String string) {
    if (astStr == null) {
      astStr = new StringBuilder();
    }
    astStr.append(string);
  }

  @Override
  public void setParent(Tree t) {
    super.setParent(t);
    resetRootInformation();
  }

  @Override
  public void addChild(Tree t) {
    super.addChild(t);
    resetRootInformation();
  }

  @Override
  public void addChildren(List kids) {
    super.addChildren(kids);
    resetRootInformation();
  }

  @Override
  public void setChild(int i, Tree t) {
    super.setChild(i, t);
    resetRootInformation();
  }

  @Override
  public void insertChild(int i, Object t) {
    super.insertChild(i, t);
    resetRootInformation();
  }

  @Override
  public Object deleteChild(int i) {
    Object ret = super.deleteChild(i);
    resetRootInformation();
    return ret;
  }

  @Override
  public void replaceChildren(int startChildIndex, int stopChildIndex, Object t) {
    super.replaceChildren(startChildIndex, stopChildIndex, t);
    resetRootInformation();
  }

  @Override
  public String toStringTree() {

    // The root might have changed because of tree modifications.
    // Compute the new root for this tree and set the astStr.
    getRootNodeWithValidAstStr();

    // If rootNotModified is false, then startIndx and endIndx will be
    // stale.
    if (startIndx >= 0 && endIndx <= rootNode.getMemoizedStringLen()) {
      return rootNode.getMemoizedSubString(startIndx, endIndx);
    }
    return toStringTree(rootNode);
  }

  private String toStringTree(AstNode rootNode) {
    Deque<AstNode> stack = new ArrayDeque<AstNode>();
    stack.push(this);

    while (!stack.isEmpty()) {
      AstNode next = stack.peek();
      if (!next.visited) {
        if (next.parent != null && next.parent.getChildCount() > 1 && next != next.parent
            .getChild(0)) {
          rootNode.addtoMemoizedString(" ");
        }

        next.rootNode = rootNode;
        next.startIndx = rootNode.getMemoizedStringLen();

        // Leaf
        if (next.children == null || next.children.size() == 0) {
          String str = next.toString();
          rootNode.addtoMemoizedString(
              next.getType() != TSParser.StringLiteral ? str.toLowerCase() : str);
          next.endIndx = rootNode.getMemoizedStringLen();
          stack.pop();
          continue;
        }

        if (!next.isNil()) {
          rootNode.addtoMemoizedString("(");
          String str = next.toString();
          rootNode.addtoMemoizedString(
              (next.getType() == TSParser.StringLiteral || null == str) ? str : str.toLowerCase());
          rootNode.addtoMemoizedString(" ");
        }

        if (next.children != null) {
          for (int i = next.children.size() - 1; i >= 0; i--) {
            stack.push((AstNode) next.children.get(i));
          }
        }

        next.visited = true;
      } else {
        if (!next.isNil()) {
          rootNode.addtoMemoizedString(")");
        }
        next.endIndx = rootNode.getMemoizedStringLen();
        next.visited = false;
        stack.pop();
      }

    }

    return rootNode.getMemoizedSubString(startIndx, endIndx);
  }

  @Override
  public AstNode getChild(int i) {
    if (children == null || i >= children.size()) {
      return null;
    }
    return (AstNode) children.get(i);
  }

}
