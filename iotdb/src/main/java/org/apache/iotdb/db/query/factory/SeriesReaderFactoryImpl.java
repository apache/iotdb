package org.apache.iotdb.db.query.factory;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSourceV2;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class SeriesReaderFactoryImpl implements ISeriesReaderFactory{

  @Override
  public IPointReader createUnSeqReader(GlobalSortedSeriesDataSourceV2 overflowSeriesDataSource,
      Filter filter) throws IOException {

    return null;
  }

  @Override
  public IAggregateReader createSeqReader(GlobalSortedSeriesDataSourceV2 overflowSeriesDataSource,
      Filter filter) throws IOException {
    return null;
  }

  @Override
  public IPointReader createSeriesReaderForMerge(TsFileResourceV2 seqFile,
      TsFileResourceV2 unseqFile, SingleSeriesExpression singleSeriesExpression,
      QueryContext context) throws IOException {
    return null;
  }

  @Override
  public List<EngineReaderByTimeStamp> createByTimestampReadersOfSelectedPaths(List<Path> paths,
      QueryContext context) {
    return null;
  }

  @Override
  public List<IPointReader> createReadersOfSelectedPaths(List<Path> paths, QueryContext context) {
    return null;
  }
}
