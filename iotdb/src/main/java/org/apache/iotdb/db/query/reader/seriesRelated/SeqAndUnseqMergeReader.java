package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class SeqAndUnseqMergeReader implements IPointReader {

  private boolean hasCachedBatchData;
  private BatchData batchData;

  protected IBatchReader seqResourceIterateReader;
  protected IPointReader unseqResourceMergeReader;

  /**
   * merge sequence reader, unsequence reader.
   */
  public SeqAndUnseqMergeReader(IBatchReader seqResourceIterateReader,
      IPointReader unseqResourceMergeReader) {
    this.seqResourceIterateReader = seqResourceIterateReader;
    this.unseqResourceMergeReader = unseqResourceMergeReader;
    this.hasCachedBatchData = false;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasNextInBatchDataOrBatchReader()) {
      return true;
    }
    // has value in pointReader
    return unseqResourceMergeReader != null && unseqResourceMergeReader.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {
    // has next in both batch reader and point reader
    if (hasNextInBothReader()) {
      long timeInPointReader = unseqResourceMergeReader.current().getTimestamp();
      long timeInBatchData = batchData.currentTime();
      if (timeInPointReader > timeInBatchData) {
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
        batchData.next();
        return timeValuePair;
      } else if (timeInPointReader == timeInBatchData) {
        batchData.next();
        return unseqResourceMergeReader.next();
      } else {
        return unseqResourceMergeReader.next();
      }
    }

    // only has next in batch reader
    if (hasNextInBatchDataOrBatchReader()) {
      TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
      batchData.next();
      return timeValuePair;
    }

    // only has next in point reader
    if (unseqResourceMergeReader != null && unseqResourceMergeReader.hasNext()) {
      return unseqResourceMergeReader.next();
    }
    return null;
  }

  private boolean hasNextInBothReader() throws IOException {
    if (!hasNextInBatchDataOrBatchReader()) {
      return false;
    }
    return unseqResourceMergeReader != null && unseqResourceMergeReader.hasNext();
  }

  private boolean hasNextInBatchDataOrBatchReader() throws IOException {
    // has value in batchData
    if (hasCachedBatchData && batchData.hasNext()) {
      return true;
    } else {
      hasCachedBatchData = false;
    }

    // has value in batchReader
    while (seqResourceIterateReader != null && seqResourceIterateReader.hasNext()) {
      batchData = seqResourceIterateReader.nextBatch();
      if (batchData.hasNext()) {
        hasCachedBatchData = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair current() throws IOException {
    throw new IOException("current() in SeriesReaderWithoutValueFilter is an empty method.");
  }

  @Override
  public void close() throws IOException {
    seqResourceIterateReader.close();
    unseqResourceMergeReader.close();
  }
}
