package org.apache.iotdb.db.engine.compaction.newcross.fragment.selector;

import org.apache.iotdb.db.engine.compaction.newcross.AbstractCrossSpaceCompactionSelector;

public class FragmentFileCompactionSelector extends AbstractCrossSpaceCompactionSelector {
  @Override
  public boolean selectAndSubmit() {
    return false;
  }
}
