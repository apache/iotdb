package org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator;

public interface ListForwardIterator {
  boolean hasNext();

  void next();
}
