package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;

import java.io.IOException;
import java.util.Map;

/** this interface provides operations on mtree */
public interface MetadataAccess {

  /** get root mnode of the mtree */
  MNode getRoot() throws MetadataException;

  /** get child of the parent */
  MNode getChild(MNode parent, String name) throws MetadataException;
  MNode getChild(MNode parent, String name, boolean lockChild) throws MetadataException;

  /** get a cloned children map instance from the parent */
  Map<String, MNode> getChildren(MNode parent) throws MetadataException;

  /** add a child to the parent */
  void addChild(MNode parent, String childName, MNode child) throws MetadataException;
  void addChild(MNode parent, String childName, MNode child, boolean lockChild) throws MetadataException;

  /** add a alias child to the parent */
  void addAlias(MNode parent, String alias, MNode child) throws MetadataException;

  /** replace a child of the parent with the newChild */
  void replaceChild(MNode parent, String measurement, MNode newChild) throws MetadataException;
  void replaceChild(MNode parent, String measurement, MNode newChild, boolean lockChild) throws MetadataException;

  /** delete a child of the parent. Collect all the MNode in subtree of this child into memory
   * Attention!!!! must unlock child Node before delete */
  MNode deleteChild(MNode parent, String childName) throws MetadataException;

  /** delete a alias child of the parent */
  void deleteAliasChild(MNode parent, String alias) throws MetadataException;

  void updateMNode(MNode mNode) throws MetadataException;

  void lockMNodeInMemory(MNode mNode) throws MetadataException;

  void releaseMNodeMemoryLock(MNode mNode) throws MetadataException;

  void sync() throws IOException;

  void createSnapshot() throws IOException;

  void clear() throws IOException;
}
