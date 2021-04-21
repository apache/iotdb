package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.cache.CacheEntry;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.metafile.PersistenceInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface MNode extends Serializable {
  boolean hasChild(String name);

  void addChild(String name, MNode child);

  MNode addChild(MNode child);

  void deleteChild(String name);

  void deleteAliasChild(String alias);

  MNode getChild(String name);

  int getMeasurementMNodeCount();

  boolean addAlias(String alias, MNode child);

  String getFullPath();

  PartialPath getPartialPath();

  MNode getParent();

  void setParent(MNode parent);

  Map<String, MNode> getChildren();

  Map<String, MNode> getAliasChildren();

  void setChildren(Map<String, MNode> children);

  void setAliasChildren(Map<String, MNode> aliasChildren);

  String getName();

  void setName(String name);

  void serializeTo(MLogWriter logWriter) throws IOException;

  void replaceChild(String measurement, MNode newChildNode);

  boolean isStorageGroup();

  boolean isMeasurement();

  boolean isLoaded();

  boolean isPersisted();

  PersistenceInfo getPersistenceInfo();

  void setPersistenceInfo(PersistenceInfo persistenceInfo);

  CacheEntry getCacheEntry();

  void setCacheEntry(CacheEntry cacheEntry);

  boolean isCached();

  void evictChild(String name);
}
