package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class SeriesReaderWithoutValueFilter implements IPointReader {

  private boolean hasCachedBatchData;
  private BatchData batchData;

  protected IBatchReader seqResourceIterateReader;
  protected IPointReader unseqResourceMergeReader;

  public SeriesReaderWithoutValueFilter(Path path, Filter timeFilter, QueryContext context)
      throws StorageEngineException, IOException {
    this(path, timeFilter, context, true);
  }

  protected SeriesReaderWithoutValueFilter(Path path, Filter timeFilter, QueryContext context,
      boolean pushdownUnseq) throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path, context);

    // reader for sequence resources
    IBatchReader seqResourceIterateReader = new SeqResourceIterateReader(
        queryDataSource.getSeriesPath(), queryDataSource.getSeqResources(), timeFilter, context);

    // reader for unsequence resources
    IPointReader unseqResourceMergeReader;
    if (pushdownUnseq) {
      unseqResourceMergeReader = new UnseqResourceMergeReader(path,
          queryDataSource.getUnseqResources(), context, timeFilter);
    } else {
      unseqResourceMergeReader = new UnseqResourceMergeReader(path,
          queryDataSource.getUnseqResources(), context, null);
    }

    this.seqResourceIterateReader = seqResourceIterateReader;
    this.unseqResourceMergeReader = unseqResourceMergeReader;
    this.hasCachedBatchData = false;
  }
  /*
    // TODO 原来的SeriesReaderFactoryImpl的createSeriesReaderWithoutValueFilter里最后有这么一段，
        现在我把它去掉了，需要确认原来这样设计是否有道理，去掉是否能正确工作，以及我认为
        这种判断应该是SeriesReaderWithoutValueFilter内部处理，而不要依赖外界Impl来摘掉。

    if (!seqResourceIterateReader.hasNext()) { //TODO here need more considerartion
      // only have unsequence data.
      return unseqResourceMergeReader;
    } else {
      // merge sequence data with unsequence data.
      return new SeriesReaderWithoutValueFilter(seqResourceIterateReader, unseqResourceMergeReader);
    }
     */


  public SeriesReaderWithoutValueFilter(IBatchReader seqResourceIterateReader,
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
    // TODO 这里会不会有重复判断 hasNextInBothReader里已经判断了hasNextInBatchDataOrBatchReader
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
