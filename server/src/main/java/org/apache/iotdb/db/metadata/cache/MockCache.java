package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;

public class MockCache implements MNodeCache {
  @Override
  public void put(String path, MNode mNode) {}

  @Override
  public boolean contains(String path) {
    return false;
  }

  @Override
  public MNode get(String path) {
    return null;
  }

  @Override
  public Collection<MNode> getAll() {
    return null;
  }

  @Override
  public void remove(String path) {}

  @Override
  public int size() {
    return 0;
  }

  @Override
  public void clear() {}
}
