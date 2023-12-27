package org.apache.iotdb.db.schemaengine.rescon;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.List;

public interface IStoreStatistics {

  void addDevice();

  void deleteDevice();

  void requestMemory(int size);

  void releaseMemory(int size);

  void requestPinnedMemResource(ICachedMNode node);

  void upgradeMemResource(ICachedMNode node);

  void releasePinnedMemResource(ICachedMNode node);

  void releaseMemResource(ICachedMNode node);

  int releaseMemResource(List<ICachedMNode> evictedNodes);

  void updatePinnedSize(int deltaSize);

  void addVolatileNode();

  void removeVolatileNode();
}
