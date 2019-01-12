
package org.apache.iotdb.db.sql.parse;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;;

public class ASTNode extends CommonTree implements Node, Serializable {
	private static final long serialVersionUID = 1L;
	private transient StringBuilder astStr;
	private transient ASTNodeOrigin origin;
	private transient int startIndx = -1;
	private transient int endIndx = -1;
	private transient ASTNode rootNode;
	private transient boolean isValidASTStr;
	private transient boolean visited = false;

	public ASTNode() {
	}

	/**
	 * Constructor.
	 *
	 * @param t
	 *            Token for the CommonTree Node
	 */
	public ASTNode(Token t) {
		super(t);
	}

	public ASTNode(ASTNode node) {
		super(node);
		this.origin = node.origin;
	}

	@Override
	public Tree dupNode() {
		return new ASTNode(this);
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

		ArrayList<Node> ret_vec = new ArrayList<Node>();
		for (int i = 0; i < super.getChildCount(); ++i) {
			ret_vec.add((Node) super.getChild(i));
		}

		return ret_vec;
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
	 * For every node in this subtree, make sure it's start/stop token's are
	 * set. Walk depth first, visit bottom up. Only updates nodes with at least
	 * one token index < 0.
	 *
	 * In contrast to the method in the parent class, this method is iterative.
	 */
	@Override
	public void setUnknownTokenBoundaries() {
		Deque<ASTNode> stack1 = new ArrayDeque<ASTNode>();
		Deque<ASTNode> stack2 = new ArrayDeque<ASTNode>();
		stack1.push(this);

		while (!stack1.isEmpty()) {
			ASTNode next = stack1.pop();
			stack2.push(next);

			if (next.children != null) {
				for (int i = next.children.size() - 1; i >= 0; i--) {
					stack1.push((ASTNode) next.children.get(i));
				}
			}
		}

		while (!stack2.isEmpty()) {
			ASTNode next = stack2.pop();

			if (next.children == null) {
				if (next.startIndex < 0 || next.stopIndex < 0) {
					next.startIndex = next.stopIndex = next.token.getTokenIndex();
				}
			} else if (next.startIndex >= 0 && next.stopIndex >= 0) {
				continue;
			} else if (next.children.size() > 0) {
				ASTNode firstChild = (ASTNode) next.children.get(0);
				ASTNode lastChild = (ASTNode) next.children.get(next.children.size() - 1);
				next.startIndex = firstChild.getTokenStartIndex();
				next.stopIndex = lastChild.getTokenStopIndex();
			}
		}
	}

	/**
	 * @return information about the object from which this ASTNode originated,
	 *         or null if this ASTNode was not expanded from an object reference
	 */
	public ASTNodeOrigin getOrigin() {
		return origin;
	}

	/**
	 * Tag this ASTNode with information about the object from which this node
	 * originated.
	 */
	public void setOrigin(ASTNodeOrigin origin) {
		this.origin = origin;
	}

	public String dump() {
		StringBuilder sb = new StringBuilder("\n");
		dump(sb);
		return sb.toString();
	}

	private StringBuilder dump(StringBuilder sb) {
		Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
		stack.push(this);
		int tabLength = 0;

		while (!stack.isEmpty()) {
			ASTNode next = stack.peek();

			if (!next.visited) {
				sb.append(StringUtils.repeat(" ", tabLength * 3));
				sb.append(next.toString());
				sb.append("\n");

				if (next.children != null) {
					for (int i = next.children.size() - 1; i >= 0; i--) {
						stack.push((ASTNode) next.children.get(i));
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

	private void getRootNodeWithValidASTStr() {

		if (rootNode != null && rootNode.parent == null && rootNode.hasValidMemoizedString()) {
			return;
		}
		ASTNode retNode = this;
		while (retNode.parent != null) {
			retNode = (ASTNode) retNode.parent;
		}
		rootNode = retNode;
		if (!rootNode.isValidASTStr) {
			rootNode.astStr = new StringBuilder();
			rootNode.toStringTree(rootNode);
			rootNode.isValidASTStr = true;
		}
		return;
	}

	private boolean hasValidMemoizedString() {
		return isValidASTStr && astStr != null;
	}

	private void resetRootInformation() {
		// Reset the previously stored rootNode string
		if (rootNode != null) {
			rootNode.astStr = null;
			rootNode.isValidASTStr = false;
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
		getRootNodeWithValidASTStr();

		// If rootNotModified is false, then startIndx and endIndx will be
		// stale.
		if (startIndx >= 0 && endIndx <= rootNode.getMemoizedStringLen()) {
			return rootNode.getMemoizedSubString(startIndx, endIndx);
		}
		return toStringTree(rootNode);
	}

	private String toStringTree(ASTNode rootNode) {
		Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
		stack.push(this);

		while (!stack.isEmpty()) {
			ASTNode next = stack.peek();
			if (!next.visited) {
				if (next.parent != null && next.parent.getChildCount() > 1 && next != next.parent.getChild(0)) {
					rootNode.addtoMemoizedString(" ");
				}

				next.rootNode = rootNode;
				next.startIndx = rootNode.getMemoizedStringLen();

				// Leaf
				if (next.children == null || next.children.size() == 0) {
					String str = next.toString();
					rootNode.addtoMemoizedString(next.getType() != TSParser.StringLiteral ? str.toLowerCase() : str);
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
						stack.push((ASTNode) next.children.get(i));
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
	public ASTNode getChild(int i) {
		if (children == null || i >= children.size()) {
			return null;
		}
		return (ASTNode) children.get(i);
	}

}
