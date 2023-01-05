package org.apache.iotdb.db.metadata.mtree.traverser.updater;

import org.apache.iotdb.commons.exception.MetadataException;

public interface Updater {
  // TODO: rename to traverse()
  void update() throws MetadataException;
}
