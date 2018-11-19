
package cn.edu.tsinghua.iotdb.sql.parse;

/**
 * Library of utility functions used in the parse code.
 *
 */
public final class ParseUtils {

	/**
	 * Performs a descent of the leftmost branch of a tree, stopping when either
	 * a node with a non-null token is found or the leaf level is encountered.
	 *
	 * @param tree
	 *            candidate node from which to start searching
	 *
	 * @return node at which descent stopped
	 */
	public static ASTNode findRootNonNullToken(ASTNode tree) {
		while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
			tree = tree.getChild(0);
		}
		return tree;
	}

	private ParseUtils() {
		// prevent instantiation
	}
}
