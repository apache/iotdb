package cn.edu.thu.tsfiledb.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfiledb.exception.MetadataArgsErrorException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

/**
 * The hierarchical struct of the Metadata Tree is implemented in this class.
 * 
 * @author Jinrui Zhang
 *
 */
public class MTree implements Serializable {

	private static final long serialVersionUID = -4200394435237291964L;
	private final String space = "    ";
	private MNode root;
	private final String separator = "\\.";

	public MTree(String rootName) {
		this.setRoot(new MNode(rootName, null, false));
	}

	public MTree(MNode root) {
		this.setRoot(root);
	}

	/**
	 * Add a path to current Metadata Tree
	 * 
	 * @param path
	 *            Format: root.node.(node)*
	 */
	public int addPath(String path, String dataType, String encoding, String[] args)
			throws PathErrorException, MetadataArgsErrorException {
		int addCount = 0;
		if (getRoot() == null) {
			throw new PathErrorException("Root node is null, please initialize root first");
		}
		String[] nodeNames = path.trim().split(separator);
		if (nodeNames.length <= 1 || !nodeNames[0].equals(getRoot().getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not right.", path));
		}

		MNode cur = getRoot();
		int i;
		for (i = 1; i < nodeNames.length - 1; i++) {
			if (!cur.hasChild(nodeNames[i])) {
				cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
				addCount++;
			}
			cur = cur.getChild(nodeNames[i]);
		}
		if (cur.hasChild(nodeNames[i])) {
			throw new PathErrorException(String.format("Timeseries %s already exists.", path));
		} else {
			TSDataType dt = TSDataType.valueOf(dataType);
			TSEncoding ed = TSEncoding.valueOf(encoding);
			MNode leaf = new MNode(nodeNames[i], cur, dt, ed);
			if (args.length > 0) {
				for (int k = 0; k < args.length; k++) {
					String[] arg = args[k].split("=");
					leaf.getSchema().putKeyValueToArgs(arg[0], arg[1]);
				}
			}
			cur.addChild(nodeNames[i], leaf);
			addCount++;
		}
		return addCount;
	}

	/**
	 * Delete one path from current Metadata Tree
	 * 
	 * @param path
	 *            Format: root.node.(node)* Notice: Path must be a complete Path
	 *            from root to leaf node.
	 */
	public void deletePath(String path) throws PathErrorException {
		String[] nodes = path.split(separator);
		if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException("Timeseries %s is not correct." + path);
		}

		MNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (!cur.hasChild(nodes[i])) {
				throw new PathErrorException(
						"Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
			}
			cur = cur.getChild(nodes[i]);
		}
		cur.getParent().deleteChild(cur.getName());
		cur = cur.getParent();
		while (cur != null && !cur.getName().equals("root") && cur.getChildren().size() == 0) {
			cur.getParent().deleteChild(cur.getName());
			cur = cur.getParent();
		}
	}

	/**
	 * Set filename to one node in the Metadata Tree Notice: This method will
	 * set the {@code MNode.dataFileName} and {@code MNode.isStorageLevel} of
	 * current node called if and only if {@code MNode.isStorageLevel} is true.
	 * 
	 * @param path
	 *            Format: root.node.(node)*
	 */
	private void setFileNameToOnePath(String path) throws PathErrorException {
		String nodes[] = path.trim().split(separator);
		if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not correct", path));
		}
		MNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (cur.hasChild(nodes[i])) {
				cur = cur.getChild(nodes[i]);
			} else {
				throw new PathErrorException(
						"Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
			}
		}

		cur.setStorageLevel(true);
		setFileNameToMNode(cur, path);
	}

	/**
	 * Set storage level for current Metadata Tree.
	 * 
	 * @param path
	 *            Format: root.node.(node)*
	 */
	public void setStorageLevel(String path) throws PathErrorException {
		String nodes[] = path.trim().split(separator);
		if (nodes.length <= 1 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not correct", path));
		}

		int level = 0;
		MNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (cur.hasChild(nodes[i])) {
				cur = cur.getChild(nodes[i]);
				level++;
			} else {
				throw new PathErrorException(
						"Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
			}
		}

		cur = getRoot().getChild(nodes[1]);
		setFileName(1, level, cur, "root." + cur.getName());
	}

	private void setFileName(int curLevel, int level, MNode node, String path) throws PathErrorException {
		if (curLevel == level) {
			setFileNameToOnePath(path);
			return;
		}
		for (MNode child : node.getChildren().values()) {
			setFileName(curLevel + 1, level, child, path + "." + child.getName());
		}
	}

	private void setFileNameToMNode(MNode node, String fileName) {
		node.setDataFileName(fileName);
		if (!node.isLeaf()) {
			for (MNode child : node.getChildren().values()) {
				setFileNameToMNode(child, fileName);
			}
		}
	}

	/**
	 * Check whether the path given exists
	 */
	public boolean hasPath(String path) {
		String[] nodes = path.split(separator);
		if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
			return false;
		}
		return hasPath(getRoot(), nodes, 1);
	}

	private boolean hasPath(MNode node, String[] nodes, int idx) {
		if (idx >= nodes.length) {
			return true;
		}
		if (nodes[idx].equals("*")) {
			boolean res = false;
			for (MNode child : node.getChildren().values()) {
				res |= hasPath(child, nodes, idx + 1);
			}
			return res;
		} else {
			if (node.hasChild(nodes[idx])) {
				return hasPath(node.getChild(nodes[idx]), nodes, idx + 1);
			}
			return false;
		}
	}

	/**
	 * Get ColumnSchema for given path. Notice: Path must be a complete Path
	 * from root to leaf node.
	 */
	public ColumnSchema getSchemaForOnePath(String path) throws PathErrorException {
		MNode leaf = getLeafByPath(path);
		return leaf.getSchema();
	}

	private MNode getLeafByPath(String path) throws PathErrorException {
		checkPath(path);
		String[] node = path.split(separator);
		MNode cur = getRoot();
		for (int i = 1; i < node.length; i++) {
			cur = cur.getChild(node[i]);
		}
		if (!cur.isLeaf()) {
			throw new PathErrorException(String.format("Timeseries %s is not the leaf node", path));
		}
		return cur;
	}

	/**
	 * Extract the DeltaObjectType from given path
	 * 
	 * @return String represents the DeltaObjectId
	 */
	public String getDeltaObjectTypeByPath(String path) throws PathErrorException {
		checkPath(path);
		String[] nodes = path.split(separator);
		if (nodes.length < 2) {
			throw new PathErrorException(String.format("Timeseries %s must have two or more nodes", path));
		}
		return nodes[0] + "." + nodes[1];
	}

	/**
	 * Check whether a path is available
	 */
	private void checkPath(String path) throws PathErrorException {
		String[] nodes = path.split(separator);
		if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not correct", path));
		}
		MNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (!cur.hasChild(nodes[i])) {
				throw new PathErrorException(
						"Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
			}
			cur = cur.getChild(nodes[i]);
		}
	}

	/**
	 * Get the file name for given path Notice: This method could be called if
	 * and only if the path includes one node whose {@code isStorageLevel} is
	 * true
	 */
	public String getFileNameByPath(String path) throws PathErrorException {
		String[] nodes = path.split(separator);
		if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not correct", path));
		}

		MNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (!cur.hasChild(nodes[i])) {
				throw new PathErrorException(
						"Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:" + nodes[i]);
			}
			if (cur.getDataFileName() != null) {
				return cur.getDataFileName();
			}
			cur = cur.getChild(nodes[i]);
		}
		if (cur.getDataFileName() != null) {
			return cur.getDataFileName();
		}
		throw new PathErrorException(String.format(
				"Timeseries %s does not set storage group, please set storage group first and then do the operation",
				path));
	}

	/**
	 * Get all paths for given path regular expression Regular expression in
	 * this method is formed by the amalgamation of path and the character '*'
	 * 
	 * @return A HashMap whose Keys are separated by the storage file name.
	 */
	public HashMap<String, ArrayList<String>> getAllPath(String pathReg) throws PathErrorException {
		HashMap<String, ArrayList<String>> paths = new HashMap<>();
		String[] nodes = pathReg.split(separator);
		if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not correct", pathReg));
		}
		if (!pathReg.equals("root")) {
			if (!findPath(getRoot(), nodes, 1, "", paths, false)) {
				throw new PathErrorException(String.format("Timeseries %s is not correct", pathReg));
			}
		} else {
			findPath(getRoot(), nodes, 1, "", paths, false);
			return paths;
		}
		return paths;
	}

	public ArrayList<String> getAllPathInList(String path) throws PathErrorException {
		ArrayList<String> res = new ArrayList<>();
		HashMap<String, ArrayList<String>> mapRet = getAllPath(path);
		for (ArrayList<String> value : mapRet.values()) {
			res.addAll(value);
		}
		return res;
	}

	/**
	 * Calculate the count of storage-level nodes included in given path
	 * 
	 * @return The total count of storage-level nodes.
	 */
	public int getFileCountForOneType(String path) throws PathErrorException {
		String nodes[] = path.split(separator);
		if (nodes.length != 2 || !nodes[0].equals(getRoot().getName()) || !getRoot().hasChild(nodes[1])) {
			throw new PathErrorException(
					"Timeseries must be " + getRoot().getName() + ". X (X is one of the nodes of root's children)");
		}
		return getFileCountForOneNode(getRoot().getChild(nodes[1]));
	}

	private int getFileCountForOneNode(MNode node) {

		if (node.isStorageLevel()) {
			return 1;
		}
		int sum = 0;
		if (!node.isLeaf()) {
			for (MNode child : node.getChildren().values()) {
				sum += getFileCountForOneNode(child);
			}
		}
		return sum;
	}

	/**
	 * Get all DeltaObject type in current Metadata Tree
	 * 
	 * @return a list contains all distinct DeltaObject type
	 */
	public ArrayList<String> getAllType() {
		ArrayList<String> res = new ArrayList<>();
		if (getRoot() != null) {
			for (String type : getRoot().getChildren().keySet()) {
				res.add(type);
			}
		}
		return res;
	}

	/**
	 * Get all delta objects for given type
	 * 
	 * @param type
	 *            DeltaObject Type
	 * @return a list contains all delta objects for given type
	 * @throws PathErrorException
	 */
	public ArrayList<String> getDeltaObjectForOneType(String type) throws PathErrorException {
		String path = getRoot().getName() + "." + type;
		checkPath(path);
		HashMap<String, Integer> deltaObjectMap = new HashMap<>();
		MNode typeNode = getRoot().getChild(type);
		putDeltaObjectToMap(getRoot().getName(), typeNode, deltaObjectMap);
		ArrayList<String> res = new ArrayList<>();
		res.addAll(deltaObjectMap.keySet());
		return res;
	}

	private void putDeltaObjectToMap(String path, MNode node, HashMap<String, Integer> deltaObjectMap) {
		if (node.isLeaf()) {
			deltaObjectMap.put(path, 1);
		} else {
			for (String child : node.getChildren().keySet()) {
				String newPath = path + "." + node.getName();
				putDeltaObjectToMap(newPath, node.getChildren().get(child), deltaObjectMap);
			}
		}
	}

	/**
	 * Get all ColumnSchemas for given delta object type
	 * 
	 * @param path
	 *            A path represented one Delta object
	 * @return a list contains all column schema
	 * @throws PathErrorException
	 */
	public ArrayList<ColumnSchema> getSchemaForOneType(String path) throws PathErrorException {
		String nodes[] = path.split(separator);
		if (nodes.length != 2 || !nodes[0].equals(getRoot().getName()) || !getRoot().hasChild(nodes[1])) {
			throw new PathErrorException(
					"Timeseries must be " + getRoot().getName() + ". X (X is one of the nodes of root's children)");
		}
		HashMap<String, ColumnSchema> leafMap = new HashMap<>();
		putLeafToLeafMap(getRoot().getChild(nodes[1]), leafMap);
		ArrayList<ColumnSchema> res = new ArrayList<>();
		res.addAll(leafMap.values());
		return res;
	}

	private void putLeafToLeafMap(MNode node, HashMap<String, ColumnSchema> leafMap) {
		if (node.isLeaf()) {
			if (!leafMap.containsKey(node.getName())) {
				leafMap.put(node.getName(), node.getSchema());
			}
			return;
		}
		for (MNode child : node.getChildren().values()) {
			putLeafToLeafMap(child, leafMap);
		}
	}

	private boolean findPath(MNode node, String[] nodes, int idx, String parent,
			HashMap<String, ArrayList<String>> paths, boolean pathExist) {
		if (node.isLeaf()) {
			String fileName = node.getDataFileName();
			String nodePath = parent + node;
			putAPath(paths, fileName, nodePath);
			pathExist = true;
			return pathExist;
		}
		String nodeReg;
		if (idx >= nodes.length) {
			nodeReg = "*";
		} else {
			nodeReg = nodes[idx];
		}

		if (!nodeReg.equals("*")) {
			if (!node.hasChild(nodeReg)) {
				
			} else {
				pathExist = findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", paths,
						pathExist);
			}
		} else {
			for (MNode child : node.getChildren().values()) {
				pathExist = findPath(child, nodes, idx + 1, parent + node.getName() + ".", paths, pathExist);
			}
		}
		return pathExist;
	}

	private void putAPath(HashMap<String, ArrayList<String>> paths, String fileName, String nodePath) {
		if (paths.containsKey(fileName)) {
			paths.get(fileName).add(nodePath);
		} else {
			ArrayList<String> pathList = new ArrayList<>();
			pathList.add(nodePath);
			paths.put(fileName, pathList);
		}
	}

	public String toString() {
		return MNodeToString(getRoot(), 0);
	}

	private String MNodeToString(MNode node, int tab) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < tab; i++) {
			builder.append(space);
		}
		builder.append(node.getName());
		if (!node.isLeaf() && node.getChildren().size() > 0) {
			builder.append(":{\n");
			int first = 0;
			for (MNode child : node.getChildren().values()) {
				if (first == 0) {
					first = 1;
				} else {
					builder.append(",\n");
				}
				builder.append(MNodeToString(child, tab + 1));
			}
			builder.append("\n");
			for (int i = 0; i < tab; i++) {
				builder.append(space);
			}
			builder.append("}");
		} else if (node.isLeaf()) {
			builder.append(":{\n");
			builder.append(String.format("%s DataType: %s,\n", getTabs(tab + 1), node.getSchema().dataType));
			builder.append(String.format("%s Encoding: %s,\n", getTabs(tab + 1), node.getSchema().encoding));
			builder.append(String.format("%s args: %s,\n", getTabs(tab + 1), node.getSchema().getArgsMap()));
			builder.append(String.format("%s FileName: %s \n", getTabs(tab + 1), node.getDataFileName()));
			builder.append(getTabs(tab));
			builder.append("}");
		}
		return builder.toString();
	}

	private String getTabs(int count) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) {
			sb.append(space);
		}
		return sb.toString();
	}

	public MNode getRoot() {
		return root;
	}

	public void setRoot(MNode root) {
		this.root = root;
	}
}
