package org.apache.iotdb.db.query.reader.seriesRelated;

import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReaderByTimestamp;

public class SeqAndUnseqReaderByTimestamp extends PriorityMergeReaderByTimestamp {

  public SeqAndUnseqReaderByTimestamp(IReaderByTimestamp seqResourceReaderByTimestamp,
      IReaderByTimestamp unseqResourceReaderByTimestamp) {
    addReaderWithPriority(seqResourceReaderByTimestamp, 1);
    addReaderWithPriority(unseqResourceReaderByTimestamp, 2);
  }
}
