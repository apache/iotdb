package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.metadisk.cache.CacheStrategy;
import org.apache.iotdb.db.metadata.metadisk.cache.LRUCacheStrategy;
import org.apache.iotdb.db.metadata.metadisk.metafile.MetaFileAccess;
import org.apache.iotdb.db.metadata.metadisk.metafile.MockMetaFile;
import org.apache.iotdb.db.metadata.metadisk.metafile.PersistenceInfo;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataDiskManager implements MetadataAccess {

  private int capacity;
  private CacheStrategy cacheStrategy;
  private static final int DEFAULT_MAX_CAPACITY = 10000;

  private MetaFileAccess metaFile;
  private static final String DEFAULT_METAFILE_PATH =
      IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
          + File.separator
          + MetadataConstant.METAFILE_PATH;

  private MNode root;

  public MetadataDiskManager() throws MetadataException {
    this(DEFAULT_MAX_CAPACITY, DEFAULT_METAFILE_PATH);
  }

  public MetadataDiskManager(int cacheSize, String metaFilePath) throws MetadataException {

    MNode root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);

    metaFile = new MockMetaFile(metaFilePath);
    //    metaFile=new MetaFile(metaFilePath);
    try {
      metaFile.write(root);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }

    capacity = cacheSize;
    cacheStrategy = new LRUCacheStrategy();
    if (capacity > 0) {
      this.root = root;
      cacheStrategy.applyChange(root);
      cacheStrategy.setModified(root, false);
    } else {
      this.root = root.getEvictionHolder();
    }
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
  public Map<String, MNode> getChildren(MNode parent) throws MetadataException {
    Map<String, MNode> result = new ConcurrentHashMap<>();
    for (String childName : parent.getChildren().keySet()) {
      MNode child = parent.getChild(childName);
      if (child.isLoaded()) {
        if (child.isCached()) {
          cacheStrategy.applyChange(child);
        } else {
          if (isCacheable(child)) {
            cacheStrategy.applyChange(child);
            cacheStrategy.setModified(child, false);
          }
        }
      } else {
        child = getMNodeFromDisk(child.getPersistenceInfo());
        parent.addChild(childName, child);
        if (isCacheable(child)) {
          cacheStrategy.applyChange(child);
          cacheStrategy.setModified(child, false);
        }
      }
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
  public void deleteChild(MNode parent, String childName) throws MetadataException {
    MNode child = parent.getChild(childName);
    if (child.isCached()) {
      cacheStrategy.remove(child);
    }
    parent.deleteChild(childName);
    try {
      if (child.isPersisted()) {
        metaFile.remove(child.getPersistenceInfo());
      }
      if (parent.isCached()) {
        cacheStrategy.setModified(parent, true);
      } else {
        metaFile.write(parent);
      }
    } catch (IOException e) {
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

  private MNode getMNodeFromDisk(PersistenceInfo persistenceInfo) throws MetadataException {
    try {
      return metaFile.read(persistenceInfo);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  private boolean isCacheable(MNode mNode) throws MetadataException {
    MNode parent = mNode.getParent();
    // parent may be evicted from the cache when checkSizeAndEviction
    return parent.isCached() && checkSizeAndEviction() && parent.isCached();
  }

  private boolean checkSizeAndEviction() throws MetadataException {
    if (capacity == 0) {
      return false;
    }
    while (cacheStrategy.getSize() >= capacity) {
      List<MNode> modifiedMNodes = cacheStrategy.evict();
      MNode evictedMNode = modifiedMNodes.remove(0);
      System.out.println(
          evictedMNode + " " + modifiedMNodes.size() + " " + cacheStrategy.getSize());
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
