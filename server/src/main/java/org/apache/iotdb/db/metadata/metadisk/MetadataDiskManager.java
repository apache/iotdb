package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.metadisk.cache.ICacheStrategy;
import org.apache.iotdb.db.metadata.metadisk.cache.LRUCacheStrategy;
import org.apache.iotdb.db.metadata.metadisk.metafile.IMetaFileAccess;
import org.apache.iotdb.db.metadata.metadisk.metafile.IPersistenceInfo;
import org.apache.iotdb.db.metadata.metadisk.metafile.MetaFile;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * this class is an instance of MetadataAccess, provides operations on a disk-based mtree. user of
 * this class has no need to consider whether the operated mnode is in memory or in disk.
 */
public class MetadataDiskManager implements IMetadataAccess {

  private static final Logger logger = LoggerFactory.getLogger(MetadataDiskManager.class);

  private int capacity;
  private ICacheStrategy cacheStrategy;

  private IMetaFileAccess metaFile;
  private String metaFilePath;
  private String mtreeSnapshotPath;
  private String mtreeSnapshotTmpPath;

  private IMNode root;

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  public MetadataDiskManager(int cacheSize) throws IOException {
    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    this.metaFilePath = schemaDir + File.separator + MetadataConstant.METAFILE_PATH;
    mtreeSnapshotPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT;
    mtreeSnapshotTmpPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT_TMP;
    capacity = cacheSize;
    cacheStrategy = new LRUCacheStrategy();
    init();
  }

  public MetadataDiskManager(int cacheSize, String metaFilePath) throws IOException {
    this.metaFilePath = metaFilePath;
    mtreeSnapshotPath = metaFilePath + ".snapshot.bin";
    mtreeSnapshotTmpPath = mtreeSnapshotPath + ".tmp";
    capacity = cacheSize;
    cacheStrategy = new LRUCacheStrategy();
    init();
  }

  private void init() throws IOException {
    IMNode root = recoverFromFile();
    cacheStrategy.lockMNode(root);
    cacheStrategy.setModified(root, false);
    this.root = root;
  }

  private IMNode recoverFromFile() throws IOException {
    File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
    if (tmpFile.exists()) {
      logger.warn("Creating MTree snapshot not successful before crashing...");
      Files.delete(tmpFile.toPath());
    }
    File snapshotFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    File metaFile = SystemFileFactory.INSTANCE.getFile(metaFilePath);
    if (metaFile.exists()) {
      Files.delete(metaFile.toPath());
    }

    IMNode root = null;
    long time = System.currentTimeMillis();
    if (snapshotFile.exists() && snapshotFile.renameTo(metaFile)) {
      try {
        this.metaFile = new MetaFile(metaFilePath);
        root = this.metaFile.readRoot();
        logger.debug(
            "spend {} ms to deserialize mtree from snapshot", System.currentTimeMillis() - time);
      } catch (IOException e) {
        // the metaFile got problem, corrupted. delete it and create new one.
        logger.warn(
            "Failed to deserialize from {} because {}. Use a new MTree.",
            snapshotFile.getPath(),
            e);
        this.metaFile.close();
        this.metaFile = null;
        Files.delete(metaFile.toPath());
      }
    }

    if (this.metaFile == null) {
      this.metaFile = new MetaFile(metaFilePath);
    }
    if (root == null) {
      root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
      this.metaFile.write(root);
    }

    return root;
  }

  @Override
  public IMNode getRoot() throws MetadataException {
    readLock.lock();
    try {
      return root;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public IMNode getChild(IMNode parent, String name) throws MetadataException {
    readLock.lock();
    try {
      IMNode result = parent.getChild(name);
      if (result == null) {
        return null;
      }
      if (result.isLoaded()) {
        if (result.isLockedInMemory()) {
          return result;
        }
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
        result = parent.addChild(result);
        if (result.isMeasurement()) {
          MeasurementMNode measurementMNode = (MeasurementMNode) result;
          if (measurementMNode.getAlias() != null) {
            parent.addAlias(measurementMNode.getAlias(), measurementMNode);
          }
        }
        if (isCacheable(result)) {
          cacheStrategy.applyChange(result);
          cacheStrategy.setModified(result, false);
        }
      }
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public IMNode getChild(IMNode parent, String name, boolean lockChild) throws MetadataException {
    if (!lockChild) {
      return getChild(parent, name);
    }
    readLock.lock();
    try {
      if (!parent.isLockedInMemory()) {
        throw new MetadataException("Parent MNode has not been locked before lock child");
      }
      IMNode result = parent.getChild(name);
      if (result == null) {
        return null;
      }
      if (!result.isLoaded()) {
        result = getMNodeFromDisk(result.getPersistenceInfo());
        result = parent.addChild(result);
        if (result.isMeasurement()) {
          MeasurementMNode measurementMNode = (MeasurementMNode) result;
          if (measurementMNode.getAlias() != null) {
            parent.addAlias(measurementMNode.getAlias(), measurementMNode);
          }
        }
        cacheStrategy.lockMNode(result);
        cacheStrategy.setModified(result, false);
      } else {
        cacheStrategy.lockMNode(result);
      }
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<String, IMNode> getChildren(IMNode parent) throws MetadataException {
    Map<String, IMNode> result = new ConcurrentHashMap<>();
    for (String childName : parent.getChildren().keySet()) {
      IMNode child = getChild(parent, childName);
      result.put(childName, child);
    }
    return result;
  }

  @Override
  public void addChild(IMNode parent, String childName, IMNode child) throws MetadataException {
    readLock.lock();
    try {
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void addChild(IMNode parent, String childName, IMNode child, boolean lockChild)
      throws MetadataException {
    if (!lockChild) {
      addChild(parent, childName, child);
      return;
    }
    readLock.lock();
    try {
      if (!parent.isLockedInMemory()) {
        throw new MetadataException("Parent MNode has not been locked before lock child");
      }
      child.setParent(parent);
      cacheStrategy.lockMNode(child);
      parent.addChild(childName, child);
      cacheStrategy.setModified(parent, true);
      cacheStrategy.setModified(child, true);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void addAlias(IMNode parent, String alias, IMNode child) throws MetadataException {
    readLock.lock();
    try {
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void replaceChild(IMNode parent, String measurement, IMNode newChild)
      throws MetadataException {
    readLock.lock();
    try {
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void replaceChild(IMNode parent, String measurement, IMNode newChild, boolean lockChild)
      throws MetadataException {
    if (!lockChild) {
      replaceChild(parent, measurement, newChild);
      return;
    }
    readLock.lock();
    try {
      if (!parent.isLockedInMemory()) {
        throw new MetadataException("Parent MNode has not been locked before lock child");
      }
      newChild.setParent(parent);
      cacheStrategy.lockMNode(newChild);
      getChild(parent, measurement);
      parent.replaceChild(measurement, newChild);
      cacheStrategy.setModified(newChild, true);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public IMNode deleteChild(IMNode parent, String childName) throws MetadataException {
    writeLock.lock();
    try {
      IMNode child = getChild(parent, childName);
      parent.deleteChild(childName);

      if (child.isCached()) {
        cacheStrategy.remove(child);
      }

      try {
        if (parent.isCached()) {
          cacheStrategy.setModified(parent, true);
        } else {
          metaFile.write(parent);
        }
        deleteChildRecursively(child);
        return child;
      } catch (IOException e) {
        throw new MetadataException(e.getMessage());
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void deleteChildRecursively(IMNode mNode) throws MetadataException {
    IMNode child;
    for (String childName : mNode.getChildren().keySet()) {
      child = mNode.getChild(childName);
      if (!child.isLoaded()) {
        child = getMNodeFromDisk(child.getPersistenceInfo());
        mNode.addChild(childName, child);
      }
      deleteChildRecursively(child);
    }
    if (mNode.isPersisted()) {
      try {
        metaFile.remove(mNode.getPersistenceInfo());
      } catch (IOException e) {
        throw new MetadataException(e.getMessage());
      }
    }
  }

  @Override
  public void deleteAliasChild(IMNode parent, String alias) throws MetadataException {
    readLock.lock();
    try {
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void updateMNode(IMNode mNode) throws MetadataException {
    readLock.lock();
    try {
      if (mNode.isDeleted()) {
        return;
      }
      if (mNode.isCached()) {
        cacheStrategy.applyChange(mNode);
        cacheStrategy.setModified(mNode, true);
      } else if (mNode.isPersisted()) {
        try {
          metaFile.write(mNode);
        } catch (IOException e) {
          throw new MetadataException(e.getMessage());
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void lockMNodeInMemory(IMNode mNode) throws MetadataException {
    readLock.lock();
    try {
      IMNode temp = mNode;
      Stack<IMNode> stack = new Stack<>();
      while (temp != root) {
        if (!temp.isCached()) {
          throw new MetadataException("Cannot lock a MNode not in cache");
        }
        stack.push(temp);
        temp = temp.getParent();
      }
      temp = stack.pop();
      cacheStrategy.lockMNode(temp);
      IMNode last;
      while (!stack.empty()) {
        last = temp;
        temp = stack.pop();
        if (!temp.isCached()) {
          cacheStrategy.unlockMNode(last);
          throw new MetadataException("Cannot lock a MNode not in cache");
        }
        cacheStrategy.lockMNode(temp);
        cacheStrategy.unlockMNode(last);
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void releaseMNodeMemoryLock(IMNode mNode) throws MetadataException {
    readLock.lock();
    try {
      if (!mNode.isCached() || !mNode.isLockedInMemory()) {
        return;
      }
      if (mNode == root) {
        return;
      }
      cacheStrategy.unlockMNode(mNode);
      checkEviction();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void sync() throws IOException {
    writeLock.lock();
    try {
      List<IMNode> modifiedMNodes = cacheStrategy.collectModified(root);
      for (IMNode mNode : modifiedMNodes) {
        metaFile.write(mNode);
        cacheStrategy.setModified(mNode, false);
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void createSnapshot() throws IOException {
    writeLock.lock();
    try {
      long time = System.currentTimeMillis();
      logger.info("Start creating MTree snapshot to {}", mtreeSnapshotPath);
      try {
        sync();
        File metaFile = SystemFileFactory.INSTANCE.getFile(metaFilePath);
        File tempFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
        Files.copy(metaFile.toPath(), tempFile.toPath());
        File snapshotFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
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
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void clear() throws IOException {
    writeLock.lock();
    try {
      cacheStrategy.clear();
      root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
      cacheStrategy.lockMNode(root);
      cacheStrategy.setModified(root, false);
      if (metaFile != null) {
        metaFile.close();
      }
      metaFile = null;
    } finally {
      writeLock.unlock();
    }
  }

  /** get mnode from disk */
  private IMNode getMNodeFromDisk(IPersistenceInfo persistenceInfo) throws MetadataException {
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
  private boolean isCacheable(IMNode mNode) throws MetadataException {
    IMNode parent = mNode.getParent();
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
    checkEviction();
    return cacheStrategy.getSize() < capacity;
  }

  /**
   * Attention!!!!!!!! This method is not synchronized. Some MNode will be evict after
   * readLock.unlock(). Since the evicted MNode could be read back from disk, this won't cause any
   * security or correctness problem.
   */
  private void checkEviction() throws MetadataException {
    if (cacheStrategy.getSize() >= capacity) {
      readLock.unlock();
      writeLock.lock();
      try {
        while (cacheStrategy.getSize() >= capacity * 0.8) {
          List<IMNode> modifiedMNodes = cacheStrategy.evict();
          if (modifiedMNodes.size() == 0) {
            break;
          }
          IMNode evictedMNode = modifiedMNodes.remove(0);
          for (IMNode mNode : modifiedMNodes) {
            try {
              metaFile.write(mNode);
              cacheStrategy.setModified(mNode, false);
            } catch (IOException e) {
              throw new MetadataException(e.getMessage());
            }
          }
          if (evictedMNode.getParent() != null) {
            evictedMNode.getParent().evictChild(evictedMNode.getName());
          }
        }
      } finally {
        writeLock.unlock();
        readLock.lock();
      }
    }
  }
}
