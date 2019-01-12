package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.PathErrorException;

/**
 * "PTree" is the shorthand for "Property Tree". One {@code PTree} consists
 * several {@code PNode}
 */
public class PTree implements Serializable {
	private static final long serialVersionUID = 2642766399323283900L;

	private PNode root;
	private MTree mTree;
	private String name;
	private String space = "    ";

	public PTree(String name, MTree mTree) {
		this.setRoot(new PNode(name, null, false));
		this.setName(name);
		this.setmTree(mTree);
	}

	public PTree(String name, PNode root, MTree mTree) {
		this.setName(name);
		this.setRoot(root);
		this.setmTree(mTree);
	}

	/**
	 * Add a seriesPath to current PTree
	 * @return The count of new added {@code PNode}
	 * @throws PathErrorException
	 */
	public int addPath(String path) throws PathErrorException {
		int addCount = 0;
		if (getRoot() == null) {
			throw new PathErrorException("Root Node is null, Please initialize root first");
		}
		String[] nodes = path.trim().split("\\.");
		if (nodes.length <= 1 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException("Input seriesPath not exist. Path: " + path);
		}

		PNode cur = getRoot();
		int i;
		for (i = 1; i < nodes.length - 1; i++) {
			if (!cur.hasChild(nodes[i])) {
				cur.addChild(nodes[i], new PNode(nodes[i], cur, false));
				addCount++;
			}
			cur = cur.getChild(nodes[i]);
		}
		if (cur.hasChild(nodes[i])) {
			throw new PathErrorException("Path already exists. Path: " + path);
		} else {
			PNode node = new PNode(nodes[i], cur, true);
			cur.addChild(node.getName(), node);
			addCount++;
		}
		return addCount;
	}

	/**
	 * Remove a seriesPath from current PTree
	 * @throws PathErrorException
	 */
	public void deletePath(String path) throws PathErrorException {
		String[] nodes = path.split("\\.");
		if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException("Path not correct. Path:" + path);
		}
		PNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (!cur.hasChild(nodes[i])) {
				throw new PathErrorException(
						"Path not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
			}
			cur = cur.getChild(nodes[i]);
		}
		cur.getParent().deleteChild(cur.getName());
	}

	/**
	 * Link a {@code MNode} to a {@code PNode} in current PTree
	 * @throws PathErrorException
	 */
	public void linkMNode(String pTreePath, String mTreePath) throws PathErrorException {
		List<String> paths = mTree.getAllPathInList(mTreePath);
		String nodes[] = pTreePath.trim().split("\\.");
		PNode leaf = getLeaf(getRoot(), nodes, 0);
		for (String p : paths) {
			leaf.linkMPath(p);
		}
	}

	/**
	 * Unlink a {@code MNode} from a {@code PNode} in current PTree
	 * @throws PathErrorException
	 */
	public void unlinkMNode(String pTreePath, String mTreePath) throws PathErrorException {
		List<String> paths = mTree.getAllPathInList(mTreePath);
		String nodes[] = pTreePath.trim().split("\\.");
		PNode leaf = getLeaf(getRoot(), nodes, 0);
		for (String p : paths) {
			leaf.unlinkMPath(p);
		}
	}

	private PNode getLeaf(PNode node, String[] nodes, int idx) throws PathErrorException {
		if (idx >= nodes.length) {
			throw new PathErrorException("PTree seriesPath not exist. ");
		}
		if (node.isLeaf()) {
			if (idx != nodes.length - 1 || !nodes[idx].equals(node.getName())) {
				throw new PathErrorException("PTree seriesPath not exist. ");
			}
			return node;
		}else{
			if (idx >= nodes.length - 1 || !node.hasChild(nodes[idx + 1])) {
				throw new PathErrorException("PTree seriesPath not exist. ");
			}
			return getLeaf(node.getChild(nodes[idx + 1]), nodes, idx + 1);
		}
	}

	/**
	 * 
	 * @param path
	 *            a seriesPath in current {@code PTree} Get all linked seriesPath in MTree
	 *            according to the given seriesPath in PTree
	 * @return Paths will be separated by the {@code MNode.dataFileName} in the
	 *         HashMap
	 * @throws PathErrorException
	 */
	public HashMap<String, ArrayList<String>> getAllLinkedPath(String path) throws PathErrorException {
		String[] nodes = path.trim().split("\\.");
		PNode leaf = getLeaf(getRoot(), nodes, 0);
		HashMap<String, ArrayList<String>> res = new HashMap<>();

		for (String MPath : leaf.getLinkedMTreePathMap().keySet()) {
			HashMap<String, ArrayList<String>> tr = getmTree().getAllPath(MPath);
			mergePathRes(res, tr);
		}
		return res;
	}

	private void mergePathRes(HashMap<String, ArrayList<String>> res, HashMap<String, ArrayList<String>> tr) {
		for (String key : tr.keySet()) {
			if (!res.containsKey(key)) {
				res.put(key, new ArrayList<String>());
			}
			for (String p : tr.get(key)) {
				if (!res.get(key).contains(p)) {
					res.get(key).add(p);
				}
			}
		}
	}

	public String toString() {
		return PNodeToString(getRoot(), 0);
	}

	private String PNodeToString(PNode node, int tab) {
		String s = "";
		for (int i = 0; i < tab; i++) {
			s += space;
		}
		s += node.getName();
		if (!node.isLeaf() && node.getChildren().size() > 0) {
			s += ":{\n";
			int first = 0;
			for (PNode child : node.getChildren().values()) {
				if (first == 0) {
					first = 1;
				} else {
					s += ",\n";
				}
				s += PNodeToString(child, tab + 1);
			}
			s += "\n";
			for (int i = 0; i < tab; i++) {
				s += space;
			}
			s += "}";
		} else if (node.isLeaf()) {
			s += ":{\n";
			String[] linkedPaths = new String[node.getLinkedMTreePathMap().values().size()];
			linkedPaths = node.getLinkedMTreePathMap().values().toArray(linkedPaths);
			for (int i = 0; i < linkedPaths.length; i++) {
				if (i != linkedPaths.length - 1) {
					s += getTabs(tab + 1) + linkedPaths[i] + ",\n";
				} else {
					s += getTabs(tab + 1) + linkedPaths[i] + "\n";
				}
			}
			s += getTabs(tab) + "}";
		}
		return s;
	}

	private String getTabs(int count) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) {
			sb.append(space);
		}
		return sb.toString();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public MTree getmTree() {
		return mTree;
	}

	public void setmTree(MTree mTree) {
		this.mTree = mTree;
	}

	public PNode getRoot() {
		return root;
	}

	public void setRoot(PNode root) {
		this.root = root;
	}
}
