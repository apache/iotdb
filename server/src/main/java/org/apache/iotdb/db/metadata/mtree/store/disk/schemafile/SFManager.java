package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaFileNotExists;
import org.apache.iotdb.db.exception.metadata.SegmentNotFoundException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * SFManager(Schema File Manager) enables MTreeStore operates MNodes on disk ignoring specific file.
 *
 * <p>This class mainly implements these functions:
 * <li>Scan target directory, recover MTree above storage group
 * <li>Extract storage group, choose corresponding file and operate on it
 * <li>Maintain nodes above storage group on MTree
 */
public class SFManager {

  public static String fileDirs =
      IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
          + File.separator
          + MetadataConstant.SCHEMA_FILE_DIR;

  // key for the path from root to sgNode
  Map<String, ISchemaFile> schemaFiles;
  IMNode root;

  private static class SFManagerHolder {
    private SFManagerHolder() {}

    private static final SFManager Instance = new SFManager();
  }

  public static SFManager getInstance() {
    return SFManagerHolder.Instance;
  }

  protected SFManager() {
    schemaFiles = new HashMap<>();
    root = new InternalMNode(null, MetadataConstant.ROOT);
  }

  // region Interfaces

  /**
   * Load all schema files under target directory, return the recovered tree.
   *
   * @return a deep copy tree corresponding to files.
   */
  public IMNode init() throws MetadataException, IOException {
    loadSchemaFiles();
    return getUpperMTree();
  }

  /**
   * Get storage group name of the parameter node, write the node with non-negative segment address
   * into corresponding file.
   *
   * @param node cannot be a MeasurementMNode
   */
  public void writeMNode(IMNode node) throws MetadataException, IOException {
    IMNode sgNode = getStorageGroupNode(node);
    String sgName = sgNode.getFullPath();

    // if SG has no file in map, load from disk; if no file on disk, init one
    if (!schemaFiles.containsKey(sgName)) {
      if (loadSchemaFileInst(sgName) == null) {
        initNewSchemaFile(sgName, sgNode.getAsStorageGroupMNode().getDataTTL());
      }
      appendStorageGroupNode(sgNode);
    }

    schemaFiles.get(sgName).writeMNode(node);
  }

  /**
   * If a storage group node, remove corresponding file and prune upper tree, otherwise remove
   * record of the node as well as segment of it if not measurement.
   *
   * @param node arbitrary instance implements IMNode
   */
  public void delete(IMNode node) throws MetadataException, IOException {
    if (node.isStorageGroup()) {
      // delete entire corresponding file
      removeSchemaFile(node.getFullPath());
      pruneStorageGroupNode(node);
    } else {
      // delete inside a schema file
      loadAndUpdateUpperTree(node);
      schemaFiles.get(getStorageGroupNode(node).getFullPath()).delete(node);
    }
  }

  /** Close corresponding files and get a new upper tree. */
  public void close() throws MetadataException, IOException {
    for (ISchemaFile file : schemaFiles.values()) {
      file.close();
    }
    schemaFiles.clear();
    root = new InternalMNode(null, MetadataConstant.ROOT);
  }

  /** Close corresponding schema file of the sgNode. */
  public void close(IMNode sgNode) throws MetadataException, IOException {
    if (!sgNode.isStorageGroup()) {
      throw new MetadataException(
          String.format(
              "Node [%s] is not a storage group node, cannot close schema file either.",
              sgNode.getFullPath()));
    }

    if (schemaFiles.containsKey(sgNode.getFullPath())) {
      schemaFiles.get(sgNode.getFullPath()).close();
      schemaFiles.remove(sgNode.getFullPath());
    }
    pruneStorageGroupNode(sgNode);
  }

  public void sync() throws MetadataException, IOException {
    for (ISchemaFile file : schemaFiles.values()) {
      file.sync();
    }
  }

  public void sync(String sgName) throws MetadataException, IOException {
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).sync();
    }
  }

  public void clear() throws MetadataException, IOException {
    for (ISchemaFile file : schemaFiles.values()) {
      file.clear();
    }
  }

  public IMNode getChildNode(IMNode parent, String childName)
      throws MetadataException, IOException {
    loadAndUpdateUpperTree(parent);
    return schemaFiles
        .get(getStorageGroupNode(parent).getFullPath())
        .getChildNode(parent, childName);
  }

  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException {
    loadAndUpdateUpperTree(parent);
    try {
      return schemaFiles.get(getStorageGroupNode(parent).getFullPath()).getChildren(parent);
    } catch (SegmentNotFoundException e) {
      // throw wrapped exception since class above SFManager shall not perceive page or segment
      // inside schema file
      throw new MetadataException(
          String.format("Node [%s] does not exists in schema file.", parent.getFullPath()));
    }
  }

  // endregion

  private void loadAndUpdateUpperTree(IMNode parent) throws MetadataException, IOException {
    IMNode sgNode = getStorageGroupNode(parent);
    String sgName = sgNode.getFullPath();
    if (!schemaFiles.containsKey(sgName)) {
      if (loadSchemaFileInst(sgName) == null) {
        throw new SchemaFileNotExists(getFilePath(sgName));
      }
      appendStorageGroupNode(sgNode);
    }
  }

  // return a copy of upper tree
  public IMNode getUpperMTree() {
    IMNode cur, rCur, rNode;
    IMNode rRoot = new InternalMNode(null, MetadataConstant.ROOT);
    Deque<IMNode> stack = new ArrayDeque<>();
    stack.push(root);
    stack.push(rRoot);
    while (stack.size() != 0) {
      rCur = stack.pop();
      cur = stack.pop();
      for (IMNode node : cur.getChildren().values()) {
        if (node.isStorageGroup()) {
          // on an upper tree, storage group node has no child
          rNode =
              new StorageGroupMNode(
                  rCur, node.getName(), node.getAsStorageGroupMNode().getDataTTL());
        } else {
          rNode = new InternalMNode(rCur, node.getName());
          stack.push(node);
          stack.push(rNode);
        }
        rCur.addChild(rNode);
      }
    }
    return rRoot;
  }

  private void loadSchemaFiles() throws MetadataException, IOException {
    File dir = new File(fileDirs);
    if (!dir.isDirectory()) {
      if (dir.exists()) {
        throw new MetadataException("Invalid path for Schema File directory.");
      }
      dir.mkdirs();
    }

    File[] files = dir.listFiles();
    for (File f : files) {
      String[] fileName = MetaUtils.splitPathToDetachedPath(f.getName());
      if (fileName[fileName.length - 1].equals(MetadataConstant.SCHEMA_FILE_SUFFIX)) {
        restoreStorageGroup(Arrays.copyOfRange(fileName, 0, fileName.length - 1));
      }
    }
  }

  private ISchemaFile loadSchemaFileInst(String sgName) throws MetadataException, IOException {
    try {
      ISchemaFile file = SchemaFile.loadSchemaFile(sgName);
      schemaFiles.put(sgName, file);
      return file;
    } catch (SchemaFileNotExists e) {
      return null;
    }
  }

  private ISchemaFile initNewSchemaFile(String sgName, long dataTTL)
      throws MetadataException, IOException {
    schemaFiles.put(sgName, SchemaFile.initSchemaFile(sgName, dataTTL));
    return schemaFiles.get(sgName);
  }

  private void removeSchemaFile(String sgName) throws MetadataException, IOException {
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).close();
      schemaFiles.remove(sgName);
    }
    File file = new File(getFilePath(sgName));
    Files.delete(Paths.get(file.toURI()));
  }

  private void restoreStorageGroup(String[] nodes) throws MetadataException, IOException {
    String sgName = String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes);
    ISchemaFile fileInst = SchemaFile.loadSchemaFile(sgName);
    appendStorageGroupNode(nodes, fileInst.init().getAsStorageGroupMNode().getDataTTL());
    schemaFiles.put(sgName, fileInst);
  }

  private IMNode getStorageGroupNode(IMNode node) throws MetadataException {
    IMNode cur = node;
    while (cur != null && !cur.isStorageGroup()) {
      cur = cur.getParent();
    }
    if (cur == null) {
      throw new MetadataException("No storage group on path: " + node.getFullPath());
    }
    return cur;
  }

  /**
   * Append corresponding storage node into upper tree, which is member of the class
   *
   * @param sgNode It is a node from MTree rather than the tree inside this class
   */
  private void appendStorageGroupNode(IMNode sgNode) throws MetadataException {
    appendStorageGroupNode(
        sgNode.getPartialPath().getNodes(), sgNode.getAsStorageGroupMNode().getDataTTL());
  }

  private void appendStorageGroupNode(String[] nodes, long dataTTL) throws MetadataException {
    if (!nodes[0].equals(root.getName())) {
      throw new MetadataException(
          "Schema File with invalid name: "
              + String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes));
    }

    IMNode cur = root;

    for (int i = 1; i < nodes.length - 1; i++) {
      if (!cur.hasChild(nodes[i])) {
        cur.addChild(new InternalMNode(cur, nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
      if (cur.isStorageGroup()) {
        throw new MetadataException(
            String.format(
                "Path [%s] cannot be a prefix and a storage group at same time.",
                cur.getFullPath()));
      }
    }
    cur.addChild(new StorageGroupMNode(cur, nodes[nodes.length - 1], dataTTL));
  }

  /**
   * Delete target storage group node, and remove all non-child node as well
   *
   * @param sgNode target storage group node from outer MTree.
   */
  private void pruneStorageGroupNode(IMNode sgNode) throws MetadataException {
    String[] nodes = sgNode.getPartialPath().getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new MetadataException(
            String.format("Path does not exists for schema files.", sgNode.getFullPath()));
      }
      cur = cur.getChild(nodes[i]);
    }
    IMNode par = cur.getParent();
    par.deleteChild(cur.getName());
    while (par != root && par.getChildren().size() == 0) {
      cur = par;
      par.deleteChild(cur.getName());
      par = par.getParent();
    }
  }

  private String getFilePath(String sgName) {
    return fileDirs
        + File.separator
        + sgName
        + IoTDBConstant.PATH_SEPARATOR
        + MetadataConstant.SCHEMA_FILE_SUFFIX;
  }
}
