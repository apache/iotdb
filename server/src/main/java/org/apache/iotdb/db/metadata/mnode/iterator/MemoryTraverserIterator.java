package org.apache.iotdb.db.metadata.mnode.iterator;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.Map;

public class MemoryTraverserIterator extends AbstractTraverserIterator {
  public MemoryTraverserIterator(
      IMTreeStore store, IMNode parent, Map<Integer, Template> templateMap)
      throws MetadataException {
    super(store, parent, templateMap);
  }
}
