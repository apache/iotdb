package org.apache.iotdb.db.engine.compaction.newcross;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;

public abstract class AbstractCrossSpaceCompactionSelector extends AbstractCompactionSelector {
  public abstract boolean selectAndSubmit();
}
