package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.metadisk.cache.CacheStrategy;
import org.apache.iotdb.db.metadata.metadisk.cache.LRUCacheStrategy;
import org.apache.iotdb.db.metadata.metadisk.metafile.MetaFile;
import org.apache.iotdb.db.metadata.metadisk.metafile.MetaFileAccess;
import org.apache.iotdb.db.metadata.metadisk.metafile.PersistenceInfo;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * this class is an instance of MetadataAccess, provides operations on a disk-based mtree. user of
 * this class has no need to consider whether the operated mnode is in memory or in disk.
 */
public class MetadataDiskManager implements MetadataAccess {

  private static final Logger logger = LoggerFactory.getLogger(MetadataDiskManager.class);

  private int capacity;
  private CacheStrategy cacheStrategy;

  private MetaFileAccess metaFile;
  private String metaFilePath;
  private String mtreeSnapshotPath;
  private String mtreeSnapshotTmpPath;

  private MNode root;

  public MetadataDiskManager(int cacheSize) throws IOException{
    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    this.metaFilePath=schemaDir+ File.separator + MetadataConstant.METAFILE_PATH;
    mtreeSnapshotPath =schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT;
    mtreeSnapshotTmpPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT_TMP;
    capacity = cacheSize;
    cacheStrategy = new LRUCacheStrategy();
    init();
  }

  public MetadataDiskManager(int cacheSize, String metaFilePath) throws IOException{
    this.metaFilePath=metaFilePath;
    mtreeSnapshotPath =metaFilePath+".snapshot.bin";
    mtreeSnapshotTmpPath = mtreeSnapshotPath+".tmp";
    capacity = cacheSize;
    cacheStrategy = new LRUCacheStrategy();
    init();
  }

  private void init() throws IOException {
    MNode root = recoverFromFile();
    if (capacity > 0) {
      this.root = root;
      cacheStrategy.applyChange(root);
      cacheStrategy.setModified(root, false);
    } else {
      this.root = root.getEvictionHolder();
    }
    cacheStrategy.lockMNode(root);
    cacheStrategy.setModified(root,false);
    this.root=root;
  }

  private MNode recoverFromFile() throws IOException{
    File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
    if (tmpFile.exists()) {
      logger.warn("Creating MTree snapshot not successful before crashing...");
      Files.delete(tmpFile.toPath());
    }
    File snapshotFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    File metaFile = SystemFileFactory.INSTANCE.getFile(metaFilePath);
    if(metaFile.exists()){
      Files.delete(metaFile.toPath());
    }

    MNode root=null;
    long time = System.currentTimeMillis();
    if (snapshotFile.exists()&&snapshotFile.renameTo(metaFile)) {
      try  {
        this.metaFile = new MetaFile(metaFilePath);
        root=this.metaFile.readRoot();
        logger.debug(
                "spend {} ms to deserialize mtree from snapshot", System.currentTimeMillis() - time);
      } catch (IOException e) {
        // the metaFile got problem, corrupted. delete it and create new one.
        logger.warn("Failed to deserialize from {} because {}. Use a new MTree.", snapshotFile.getPath(),e);
        this.metaFile.close();
        this.metaFile = null;
        Files.delete(metaFile.toPath());
      }
    }

    if(this.metaFile==null){
      this.metaFile = new MetaFile(metaFilePath);
    }
    if(root==null) {
      root=new InternalMNode(null, IoTDBConstant.PATH_ROOT);
      this.metaFile.write(root);
    }

    return root;
  }

  @Override
  public MNode getRoot() throws MetadataException {
    MNode root = this.root;
    if (root.isLoaded()) {
      cacheStrategy.applyChange(root);
    } else {
      root = getMNodeFromDisk(root.getPersistenceInfo());
      if (checkSizeAndEviction()) {
        this.root = root;
        cacheStrategy.applyChange(root);
        cacheStrategy.setModified(root, false);
      }
    }
    return root;
  }

  @Override
  public MNode getChild(MNode parent, String name) throws MetadataException {
    MNode result = parent.getChild(name);
    if (result == null) {
      return null;
    }
    if (result.isLoaded()) {
      if (result.isCached()) {
        cacheStrategy.applyChange(result);
      } else {
        if (isCacheable(result)) {
          cacheStrategy.applyChange(result);
          cacheStrategy.setModified(result, false);
        }
      }
    } else {
      result = getMNodeFromDisk(result.getPersistenceInfo());
      parent.addChild(name, result);
      if (isCacheable(result)) {
        cacheStrategy.applyChange(result);
        cacheStrategy.setModified(result, false);
      }
    }
    return result;
  }

  @Override
  public MNode getChild(MNode parent, String name, boolean lockChild) throws MetadataException {
    if(!lockChild){
      return getChild(parent,name);
    }
    if(!parent.isLockedInMemory()){
      throw new MetadataException("Parent MNode has not been locked before lock child");
    }
    MNode result = parent.getChild(name);
    if (result == null) {
      return null;
    }
    if(!result.isLoaded()){
      result = getMNodeFromDisk(result.getPersistenceInfo());
      parent.addChild(name, result);
    }
    cacheStrategy.lockMNode(result);
    return result;
  }

  @Override
  public Map<String, MNode> getChildren(MNode parent) throws MetadataException {
    Map<String, MNode> result = new ConcurrentHashMap<>();
    for (String childName : parent.getChildren().keySet()) {
      MNode child = getChild(parent,childName);
      result.put(childName, child);
    }
    return result;
  }

  @Override
  public void addChild(MNode parent, String childName, MNode child) throws MetadataException {
    if (parent.hasChild(childName)) {
      return;
    }
    child.setParent(parent);
    if (isCacheable(child)) {
      parent.addChild(childName, child);
      cacheStrategy.setModified(parent, true);
      cacheStrategy.applyChange(child);
      cacheStrategy.setModified(child, true);
    } else {
      try {
        parent.addChild(childName, child);
        metaFile.write(child);
        metaFile.write(parent);
      } catch (IOException e) {
        throw new MetadataException(e.getMessage());
      }
    }
  }

  @Override
  public void addChild(MNode parent, String childName, MNode child, boolean lockChild) throws MetadataException {
    if(!lockChild){
      addChild(parent,childName,child);
      return;
    }
    if(!parent.isLockedInMemory()){
      throw new MetadataException("Parent MNode has not been locked before lock child");
    }
    if (parent.hasChild(childName)) {
      return;
    }
    child.setParent(parent);
    parent.addChild(childName, child);
    cacheStrategy.lockMNode(child);
    cacheStrategy.setModified(child, true);
    cacheStrategy.setModified(parent, true);
  }

  @Override
  public void addAlias(MNode parent, String alias, MNode child) throws MetadataException {
    child.setParent(parent);
    if (child.isCached()) {
      parent.addAlias(alias, child);
      cacheStrategy.setModified(parent, true);
      cacheStrategy.applyChange(child);
      cacheStrategy.setModified(child, true);
    } else {
      try {
        parent.addAlias(alias, child);
        metaFile.write(child);
        metaFile.write(parent);
      } catch (IOException e) {
        throw new MetadataException(e.getMessage());
      }
    }
  }

  @Override
  public void replaceChild(MNode parent, String measurement, MNode newChild)
      throws MetadataException {
    getChild(parent, measurement);
    parent.replaceChild(measurement, newChild);
    if (newChild.isCached()) {
      cacheStrategy.applyChange(newChild);
      cacheStrategy.setModified(newChild, true);
    } else {
      if (isCacheable(newChild)) {
        cacheStrategy.applyChange(newChild);
        cacheStrategy.setModified(newChild, true);
      } else {
        try {
          metaFile.write(newChild);
        } catch (IOException e) {
          throw new MetadataException(e.getMessage());
        }
      }
    }
  }

  @Override
  public void replaceChild(MNode parent, String measurement, MNode newChild, boolean lockChild) throws MetadataException {
    if(!lockChild){
      replaceChild(parent,measurement,newChild);
      return;
    }
    if(!parent.isLockedInMemory()){
      throw new MetadataException("Parent MNode has not been locked before lock child");
    }
    getChild(parent, measurement);
    parent.replaceChild(measurement, newChild);
    cacheStrategy.lockMNode(newChild);
    cacheStrategy.setModified(newChild,true);
  }

  @Override
  public void deleteChild(MNode parent, String childName) throws MetadataException {
    MNode child = parent.getChild(childName);
    parent.deleteChild(childName);

    if (child.isCached()) {
      if(child.isLockedInMemory()){
        cacheStrategy.unlockMNode(child);
      }
      cacheStrategy.remove(child);
    }
    PersistenceInfo persistenceInfo=child.getPersistenceInfo();
    child.setPersistenceInfo(null);

    try {
      if (parent.isCached()) {
        cacheStrategy.setModified(parent, true);
      } else {
        metaFile.write(parent);
      }
      if (persistenceInfo!=null) {
        metaFile.remove(persistenceInfo);
      }
    }catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  @Override
  public void deleteAliasChild(MNode parent, String alias) throws MetadataException {
    parent.deleteAliasChild(alias);
    if (parent.isCached()) {
      cacheStrategy.setModified(parent, true);
    } else {
      try {
        metaFile.write(parent);
      } catch (IOException e) {
        throw new MetadataException(e.getMessage());
      }
    }
  }

  @Override
  public void updateMNode(MNode mNode) throws MetadataException {
    if(mNode.isDeleted()){
      return;
    }
    if(mNode.isCached()){
      cacheStrategy.applyChange(mNode);
      cacheStrategy.setModified(mNode,true);
    }else if(mNode.isPersisted()){
      try {
        metaFile.write(mNode);
      } catch (IOException e) {
        throw new MetadataException(e.getMessage());
      }
    }
  }

  @Override
  public void lockMNodeInMemory(MNode mNode) throws MetadataException{
    if(!mNode.isCached()){
      throw new MetadataException("Cannot lock a MNode not in cache");
    }
    if(mNode==root){
      return;
    }
    cacheStrategy.lockMNode(mNode);
  }

  @Override
  public void releaseMNodeMemoryLock(MNode mNode) {
    if(mNode==root){
      return;
    }
    cacheStrategy.unlockMNode(mNode);
  }

  @Override
  public void sync() throws IOException {
    List<MNode> modifiedMNodes = cacheStrategy.collectModified(root);
    for (MNode mNode : modifiedMNodes) {
      metaFile.write(mNode);
      cacheStrategy.setModified(mNode, false);
    }
  }

  @Override
  public void createSnapshot() throws IOException {
    long time = System.currentTimeMillis();
    logger.info("Start creating MTree snapshot to {}", mtreeSnapshotPath);
    try {
      sync();
      File metaFile= SystemFileFactory.INSTANCE.getFile(metaFilePath);
      File tempFile=SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
      Files.copy(metaFile.toPath(),tempFile.toPath());
      File snapshotFile=SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
      if (snapshotFile.exists()) {
        Files.delete(snapshotFile.toPath());
      }
      if (tempFile.renameTo(snapshotFile)) {
        logger.info(
                "Finish creating MTree snapshot to {}, spend {} ms.",
                mtreeSnapshotPath,
                System.currentTimeMillis() - time);
      }
    } catch (IOException e) {
      logger.warn("Failed to create MTree snapshot to {}", mtreeSnapshotPath, e);
      if (SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).exists()) {
        try {
          Files.delete(SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).toPath());
        } catch (IOException e1) {
          logger.warn("delete file {} failed: {}", mtreeSnapshotTmpPath, e1.getMessage());
        }
      }
    }
  }

  @Override
  public void clear() throws IOException {
    root=root.getEvictionHolder();
    cacheStrategy.clear();
    cacheStrategy = null;
    metaFile.close();
    metaFile = null;
  }

  /** get mnode from disk */
  private MNode getMNodeFromDisk(PersistenceInfo persistenceInfo) throws MetadataException {
    try {
      return metaFile.read(persistenceInfo);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * whether a mnode is able to be cached
   *
   * <p>only if the cache has enough space and the parent of the mnode is cached, the mnode will be
   * cached.
   */
  private boolean isCacheable(MNode mNode) throws MetadataException {
    MNode parent = mNode.getParent();
    // parent may be evicted from the cache when checkSizeAndEviction
    return parent.isCached() && checkSizeAndEviction() && parent.isCached();
  }

  /**
   * when a new mnode needed to be cached, this function should be invoked first to check whether
   * the space of cache is enough and when not, cache eviction will be excuted
   */
  private boolean checkSizeAndEviction() throws MetadataException {
    if (capacity == 0) {
      return false;
    }
    while (cacheStrategy.getSize() >= capacity) {
      List<MNode> modifiedMNodes = cacheStrategy.evict();
      if(modifiedMNodes.size()==0){
        break;
      }
      MNode evictedMNode = modifiedMNodes.remove(0);
      for (MNode mNode : modifiedMNodes) {
        try {
          metaFile.write(mNode);
          cacheStrategy.setModified(mNode, false);
        } catch (IOException e) {
          throw new MetadataException(e.getMessage());
        }
      }
      if (evictedMNode.getParent() != null) {
        evictedMNode.getParent().evictChild(evictedMNode.getName());
      } else {
        // only root's parent is null
        root = root.getEvictionHolder();
      }
    }
    return true;
  }
}
