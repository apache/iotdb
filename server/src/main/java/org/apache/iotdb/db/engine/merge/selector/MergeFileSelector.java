package org.apache.iotdb.db.engine.merge.selector;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

/**
 * MergeFileSelector selects a set of files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget.
 */
public interface MergeFileSelector {

  List[] select() throws MergeException;

  int getConcurrentMergeNum();
}
