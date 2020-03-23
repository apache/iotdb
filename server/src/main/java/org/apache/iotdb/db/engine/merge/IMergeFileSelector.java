package org.apache.iotdb.db.engine.merge;

import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;

public interface IMergeFileSelector {

  Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException;

}
