package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaFileNotExists;
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
 * Schema File Manager enables upper class operates persistent schema files ignoring specific file.
 * <p>
 * This class implements these functions:
 * <li> scan target directory, recover MTree above storage group
 * <li> extract storage group, choose corresponding file
 */
public class SFManager {

  public static String fileDirs = IoTDBDescriptor.getInstance().getConfig().getSchemaDir() + File.separator + MetadataConstant.SCHEMA_FILE_DIR;

  Map<String, ISchemaFile> schemaFiles;
  IMNode root;

  private static class SFManagerHolder {
    private SFManagerHolder() {
    }

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

  public void init() throws MetadataException {
    loadSchemaFiles();
  }

  public void writeMNode(IMNode node) throws MetadataException, IOException{
    IMNode sgNode = getStorageGroupNode(node);
    String sgName = sgNode.getFullPath();

    // if SG has no file in map, load from disk; if no file on disk, init one
    if (!schemaFiles.containsKey(sgName)) {
      if (loadSchemaFileInst(sgName) == null) {
        initNewSchemaFile(sgName, sgNode.getAsStorageGroupMNode().getDataTTL());
      }
      appendUpperTree(sgNode.getPartialPath().getNodes());
    }

    schemaFiles.get(sgName).writeMNode(node);
  }

  public void delete(IMNode node) throws MetadataException, IOException {
    if (node.isStorageGroup()) {
      // delete entire corresponding file
      removeSchemaFile(node.getFullPath());
      pruneUpperTree(node.getPartialPath().getNodes());
    } else {
      // delete inside a schema file
      loadAndUpdateUpperTree(node);
      schemaFiles.get(getStorageGroupNode(node).getName()).delete(node);
    }
  }

  public void close() throws MetadataException, IOException{
    for (ISchemaFile file: schemaFiles.values()) {
      file.close();
    }
    schemaFiles.clear();
  }

  public void close(String sgName) throws MetadataException, IOException{
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).close();
      schemaFiles.remove(sgName);
    }
  }

  public void sync() throws MetadataException, IOException {
    for (ISchemaFile file: schemaFiles.values()) {
      file.sync();
    }
  }

  public void sync(String sgName) throws MetadataException, IOException {
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).sync();
    }
  }

  public void clear() throws MetadataException, IOException {
    for (ISchemaFile file: schemaFiles.values()) {
      file.clear();
    }
  }

  public IMNode getChildNode(IMNode parent, String childName) throws MetadataException, IOException{
    loadAndUpdateUpperTree(parent);
    return schemaFiles.get(getStorageGroupNode(parent).getName()).getChildNode(parent, childName);
  }

  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException {
    loadAndUpdateUpperTree(parent);
    return schemaFiles.get(getStorageGroupNode(parent).getName()).getChildren(parent);
  }


  // endregion

  private void loadAndUpdateUpperTree(IMNode parent) throws MetadataException, IOException {
    IMNode sgNode = getStorageGroupNode(parent);
    String sgName = sgNode.getFullPath();
    if (!schemaFiles.containsKey(sgName)) {
      if (loadSchemaFileInst(sgName) == null) {
        throw new SchemaFileNotExists(getFilePath(sgName));
      }
      appendUpperTree(sgNode.getPartialPath().getNodes());
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
      for(IMNode node : cur.getChildren().values()) {
        if (node.isStorageGroup()) {
          // on an upper tree, storage group node has no child
          rNode = new StorageGroupMNode(rCur, node.getName(), node.getAsStorageGroupMNode().getDataTTL());
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

  private void loadSchemaFiles() throws MetadataException {
    File dir = new File(fileDirs);
    if (!dir.isDirectory()) {
      throw new MetadataException("Invalid path for Schema File directory.");
    }

    File[] files = dir.listFiles();
    for (File f : files) {
      String[] fileName = MetaUtils.splitPathToDetachedPath(f.getName());
      if (fileName[fileName.length - 1].equals(MetadataConstant.SCHEMA_FILE_SUFFIX)) {
        restoreStorageGroup(Arrays.copyOfRange(fileName, 0, fileName.length - 1));
      }
    }
  }

  private ISchemaFile loadSchemaFileInst(String sgName) throws MetadataException, IOException{
    try {
      ISchemaFile file = SchemaFile.loadSchemaFile(sgName);
      schemaFiles.put(sgName, file);
      return file;
    } catch (SchemaFileNotExists e) {
      return null;
    }
  }

  private ISchemaFile initNewSchemaFile(String sgName, long dataTTL) throws MetadataException, IOException{
    schemaFiles.put(sgName, SchemaFile.initSchemaFile(sgName, dataTTL));
    return schemaFiles.get(sgName);
  }

  private void removeSchemaFile(String sgName) throws MetadataException, IOException{
    if (schemaFiles.containsKey(sgName)) {
      schemaFiles.get(sgName).close();
      schemaFiles.remove(sgName);
    }
    File file = new File(getFilePath(sgName));
    Files.delete(Paths.get(file.toURI()));
  }

  private void restoreStorageGroup(String[] nodes) throws MetadataException{
    String sgName = String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes);
    if (!nodes[0].equals(root.getName())) {
      throw new MetadataException("Schema File with invalid name: " + sgName);
    }
    IMNode cur = root;
    for (int i = 1; i < nodes.length - 1; i++) {
      if (cur.isStorageGroup()) {
        throw new MetadataException(String.format("Path [%s] cannot be a prefix and a storage group at same time.", sgName));
      }
      if (!cur.hasChild(nodes[i])) {
        cur.addChild(new InternalMNode(cur, nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
    }
    try {
      ISchemaFile fileInst = SchemaFile.loadSchemaFile(sgName);
      IMNode sgNode = fileInst.init();
      sgNode.setParent(cur);
      cur.addChild(sgNode);
      schemaFiles.put(sgName, fileInst);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  private IMNode getStorageGroupNode(IMNode node) throws MetadataException{
    IMNode cur = node;
    while (cur != null && !cur.isStorageGroup()) {
      cur = cur.getParent();
    }
    if (cur == null) {
      throw new MetadataException("No storage group on path: " + node.getFullPath());
    }
    return cur;
  }

  private void appendUpperTree(String[] pathNodes) {

  }

  private void pruneUpperTree(String[] pathNodes) {

  }

  private String getFilePath(String sgName) {
    return fileDirs + File.pathSeparator + sgName + IoTDBConstant.PATH_SEPARATOR + MetadataConstant.SCHEMA_FILE_SUFFIX;
  }


}
