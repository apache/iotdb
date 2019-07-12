package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceReaderByTimestamp;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceReaderByTimestamp;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.Path;

public class SeriesReaderByTimestamp extends PriorityMergeReaderByTimestamp {

  public SeriesReaderByTimestamp(Path path, QueryContext context)
      throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path, context);

    // reader for sequence resources
    SeqResourceReaderByTimestamp seqResourceReaderByTimestamp = new SeqResourceReaderByTimestamp(
        path, queryDataSource.getSeqResources(), context);

    // reader for unsequence resources
    UnseqResourceReaderByTimestamp unseqResourceReaderByTimestamp = new UnseqResourceReaderByTimestamp(
        path, queryDataSource.getUnseqResources(), context);

    addReaderWithPriority(seqResourceReaderByTimestamp, 1);
    addReaderWithPriority(unseqResourceReaderByTimestamp, 2);
  }
}
