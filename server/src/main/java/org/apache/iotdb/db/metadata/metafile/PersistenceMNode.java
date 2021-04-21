package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.cache.CacheEntry;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;
import java.util.Map;

class PersistenceMNode implements PersistenceInfo, MNode {

  /** offset in metafile */
  private long position;

  public PersistenceMNode() {}

  public PersistenceMNode(long position) {
    this.position = position;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public void setPosition(long position) {
    this.position = position;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public boolean isLoaded() {
    return false;
  }

  @Override
  public boolean isPersisted() {
    return position!=0;
  }

  @Override
  public PersistenceInfo getPersistenceInfo() {
    return this;
  }

  @Override
  public void setPersistenceInfo(PersistenceInfo persistenceInfo) {
    this.position=persistenceInfo.getPosition();
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public void addChild(String name, MNode child) {

  }

  @Override
  public MNode addChild(MNode child) {
    return null;
  }

  @Override
  public void deleteChild(String name) {

  }

  @Override
  public void deleteAliasChild(String alias) {

  }

  @Override
  public MNode getChild(String name) {
    return null;
  }

  @Override
  public int getMeasurementMNodeCount() {
    return 0;
  }

  @Override
  public boolean addAlias(String alias, MNode child) {
    return false;
  }

  @Override
  public String getFullPath() {
    return null;
  }

  @Override
  public PartialPath getPartialPath() {
    return null;
  }

  @Override
  public MNode getParent() {
    return null;
  }

  @Override
  public void setParent(MNode parent) {

  }

  @Override
  public Map<String, MNode> getChildren() {
    return null;
  }

  @Override
  public Map<String, MNode> getAliasChildren() {
    return null;
  }

  @Override
  public void setChildren(Map<String, MNode> children) {

  }

  @Override
  public void setAliasChildren(Map<String, MNode> aliasChildren) {

  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void setName(String name) {

  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {

  }

  @Override
  public void replaceChild(String measurement, MNode newChildNode) {

  }

  @Override
  public CacheEntry getCacheEntry() {
    return null;
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {

  }

  @Override
  public boolean isCached() {
    return false;
  }

  @Override
  public void evictChild(String name) {

  }

  @Override
  public MNode clone() {
    return new PersistenceMNode(position);
  }
}
