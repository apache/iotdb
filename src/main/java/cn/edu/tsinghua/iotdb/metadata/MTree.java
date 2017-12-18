package cn.edu.tsinghua.iotdb.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

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
		this.root = new MNode(rootName, null, false);
	}

	public MTree(MNode root) {
		this.root = root;
	}

	/**
	 * add timeseries, it should check whether path exists.
	 * 
	 * @param timeseriesPath
	 *            - A full path
	 * @param dataType
	 * @param encoding
	 * @param args
	 * @throws PathErrorException
	 */
	public void addTimeseriesPath(String timeseriesPath, String dataType, String encoding, String[] args)
			throws PathErrorException {
		String[] nodeNames = timeseriesPath.trim().split(separator);
		if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
			throw new PathErrorException(String.format("Timeseries %s is not right.", timeseriesPath));
		}
		int i = 1;
		MNode cur = root;
		String levelPath = null;
		while (i < nodeNames.length) {
			String nodeName = nodeNames[i];
			if (i == nodeNames.length - 1) {
				cur.setDataFileName(levelPath);
				break;
			}
			if (cur.isStorageLevel()) {
				levelPath = cur.getDataFileName();
			}
			if (!cur.hasChild(nodeName)) {
				if (cur.isLeaf()) {
					throw new PathErrorException(
							String.format("The Node [%s] is left node, the timeseries %s can't be created",
									cur.getName(), timeseriesPath));
				}
				cur.addChild(nodeName, new MNode(nodeName, cur, false));
			}
			cur.setDataFileName(levelPath);
			cur = cur.getChild(nodeName);
			if (levelPath == null) {
				levelPath = cur.getDataFileName();
			}
			i++;
		}
		TSDataType dt = TSDataType.valueOf(dataType);
		TSEncoding ed = TSEncoding.valueOf(encoding);
		MNode leaf = new MNode(nodeNames[nodeNames.length - 1], cur, dt, ed);
		if (args.length > 0) {
			for (int k = 0; k < args.length; k++) {
				String[] arg = args[k].split("=");
				leaf.getSchema().putKeyValueToArgs(arg[0], arg[1]);
			}
		}
		levelPath = cur.getDataFileName();
		leaf.setDataFileName(levelPath);
		if (cur.isLeaf()) {
			throw new PathErrorException(String.format("The Node [%s] is left node, the timeseries %s can't be created",
					cur.getName(), timeseriesPath));
		}
		cur.addChild(nodeNames[nodeNames.length - 1], leaf);
	}

	/**
	 * 
	 * @param path
	 *            -path not necessarily the whole path (possibly a prefix of a
	 *            sequence)
	 * @return
	 */
	public boolean isPathExist(String path) {
		String[] nodeNames = path.trim().split(separator);
		MNode cur = root;
		int i = 0;
		while (i < nodeNames.length - 1) {
			String nodeName = nodeNames[i];
			if (cur.getName().equals(nodeName)) {
				i++;
				nodeName = nodeNames[i];
				if (cur.hasChild(nodeName)) {
					cur = cur.getChild(nodeName);
				} else {
					return false;
				}
			} else {
				return false;
			}
		}
		if (cur.getName().equals(nodeNames[i])) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * make sure check path before setting storage group
	 * 
	 * @param path
	 * @throws PathErrorException
	 */
	public void setStorageGroup(String path) throws PathErrorException {
		String[] nodeNames = path.split(separator);
		MNode cur = root;
		if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
			throw new PathErrorException(String.format("The storage group can't be set to the %s node", path));
		}
		int i = 1;
		while (i < nodeNames.length - 1) {
			MNode temp = cur.getChild(nodeNames[i]);
			if (temp == null) {
				// add one child node
				cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
			} else if (temp.isStorageLevel()) {
				// before set storage group should check the path exist or not
				// throw exception
				throw new PathErrorException(String.format("The prefix of %s has been set to the storage group.", path));
			}
			cur = cur.getChild(nodeNames[i]);
			i++;
		}
		MNode temp = cur.getChild(nodeNames[i]);
		if (temp == null) {
			cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
		} else {
			throw new PathErrorException(
					String.format("The path of %s already exist, it can't be set to the storage group", path));
		}
		cur = cur.getChild(nodeNames[i]);
		cur.setStorageLevel(true);
		setDataFileName(path, cur);
	}

	/**
	 * Check whether set file path for this node or not. If not, throw an
	 * exception
	 * 
	 * @param node
	 * @throws PathErrorException
	 */
	private void checkStorageGroup(MNode node) throws PathErrorException {
		if (node.getDataFileName() != null) {
			throw new PathErrorException(String.format("The storage group %s has been set", node.getDataFileName()));
		}
		if (node.getChildren() == null) {
			return;
		}
		for (MNode child : node.getChildren().values()) {
			checkStorageGroup(child);
		}
	}

	private void setDataFileName(String path, MNode node) {
		node.setDataFileName(path);
		if (node.getChildren() == null) {
			return;
		}
		for (MNode child : node.getChildren().values()) {
			setDataFileName(path, child);
		}
	}

	/**
	 * Delete one path from current Metadata Tree
	 * 
	 * @param path
	 *            Format: root.node.(node)* Notice: Path must be a complete Path
	 *            from root to leaf node.
	 */
	public String deletePath(String path) throws PathErrorException {
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

		// if the storage group node is deleted, the dataFileName should be
		// return
		String dataFileName = null;
		if (cur.isStorageLevel())
			dataFileName = cur.getDataFileName();
		cur.getParent().deleteChild(cur.getName());
		cur = cur.getParent();
		while (cur != null && !cur.getName().equals("root") && cur.getChildren().size() == 0) {
			if (cur.isStorageLevel()) {
				dataFileName = cur.getDataFileName();
				return dataFileName;
			}
			cur.getParent().deleteChild(cur.getName());
			cur = cur.getParent();
		}

		return dataFileName;
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
	 * <p>
	 * Get the storage group path from the path
	 * </p>
	 * 
	 * @param path
	 * @return String storage group path
	 * @throws PathErrorException
	 */
	public String getFileNameByPath(String path) throws PathErrorException {

		String[] nodes = path.split(separator);
		MNode cur = getRoot();
		for (int i = 1; i < nodes.length; i++) {
			if (cur == null) {
				throw new PathErrorException(
						String.format("The prefix of the path %s is not one storage group path", path));
			} else if (cur.isStorageLevel()) {
				return cur.getDataFileName();
			} else {
				cur = cur.getChild(nodes[i]);
			}
		}
		if(cur.isStorageLevel()){
			return cur.getDataFileName();
		}
		throw new PathErrorException(String.format("The prefix of the path %s is not one storage group path", path));
	}

	/**
	 * <p>
	 * Check the prefix of this path is storage group path
	 * </p>
	 * 
	 * @param path
	 * @return true the prefix of this path is storage group path false the
	 *         prefix of this path is not storage group path
	 */
	public boolean checkFileNameByPath(String path) {

		String[] nodes = path.split(separator);
		MNode cur = getRoot();
		for (int i = 1; i <= nodes.length; i++) {
			if (cur == null) {
				return false;
			} else if (cur.isStorageLevel()) {
				return true;
			} else {
				cur = cur.getChild(nodes[i]);
			}
		}
		return false;
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
		findPath(getRoot(), nodes, 1, "", paths);
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
	/**
	 * <p>Get all ColumnSchemas for the filenode path</p>
	 * @param path
	 * @return ArrayList<ColumnSchema> The list of the schema
	 */
	public ArrayList<ColumnSchema> getSchemaForOneFileNode(String path){
		
		String nodes[] = path.split(separator);
		HashMap<String, ColumnSchema> leafMap = new HashMap<>();
		MNode cur = getRoot();
		for(int i = 1;i<nodes.length;i++){
			cur = cur.getChild(nodes[i]);
		}
		// cur is the storage group node
		putLeafToLeafMap(cur, leafMap);
		ArrayList<ColumnSchema> res = new ArrayList<>();
		res.addAll(leafMap.values());
		return res;
	}
	
	public Map<String, ColumnSchema> getSchemaMapForOneFileNode(String path){
		String nodes[] = path.split(separator);
		MNode cur = getRoot();
		for(int i = 1;i<nodes.length;i++){
			cur = cur.getChild(nodes[i]);
		}
		return cur.getSchemaMap();
	}
	
	public Map<String, Integer> getNumSchemaMapForOneFileNode(String path){
		String nodes[] = path.split(separator);
		MNode cur = getRoot();
		for(int i = 1;i<nodes.length;i++){
			cur = cur.getChild(nodes[i]);
		}
		return cur.getNumSchemaMap();
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

	private void findPath(MNode node, String[] nodes, int idx, String parent,
			HashMap<String, ArrayList<String>> paths) {
		if (node.isLeaf()) {
			if (nodes.length <= idx) {
				String fileName = node.getDataFileName();
				String nodePath = parent + node;
				putAPath(paths, fileName, nodePath);
			}
			return;
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
				findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", paths);
			}
		} else {
			for (MNode child : node.getChildren().values()) {
				findPath(child, nodes, idx + 1, parent + node.getName() + ".", paths);
			}
		}
		return;
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
			builder.append(String.format("%s StorageGroup: %s \n", getTabs(tab + 1), node.getDataFileName()));
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
}
