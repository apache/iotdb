package org.apache.iotdb.db.pipe.consensus.deletion.persist;

import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;

public interface DeletionBuffer extends AutoCloseable {
  void start();

  void close();

  void registerDeletionResource(DeletionResource deletionResource);

  boolean isAllDeletionFlushed();
}
