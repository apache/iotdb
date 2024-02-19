package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MNodeNewChildBuffer extends MNodeChildBuffer {
  @Nullable
  @Override
  public synchronized ICachedMNode put(String key, ICachedMNode value) {
    if (receivingBuffer == null) {
      receivingBuffer = new ConcurrentHashMap<>();
    }
    totalSize++;
    return receivingBuffer.put(key, value);
  }

  @Nullable
  @Override
  public synchronized ICachedMNode putIfAbsent(String key, ICachedMNode value) {
    ICachedMNode result = get(receivingBuffer, key);
    if (result == null) {
      if (receivingBuffer == null) {
        receivingBuffer = new ConcurrentHashMap<>();
      }
      totalSize++;
      result = receivingBuffer.put(key, value);
    }
    return result;
  }

  @Override
  public synchronized void putAll(@Nonnull Map<? extends String, ? extends ICachedMNode> m) {
    if (receivingBuffer == null) {
      receivingBuffer = new ConcurrentHashMap<>();
    }
    totalSize += m.size();
    receivingBuffer.putAll(m);
  }

  @Override
  public synchronized ICachedMNode removeFromFlushingBuffer(Object key) {
    if (flushingBuffer == null) {
      return null;
    }
    ICachedMNode result = flushingBuffer.remove(key);
    if (result != null) {
      totalSize--;
    }
    return result;
  }
}
