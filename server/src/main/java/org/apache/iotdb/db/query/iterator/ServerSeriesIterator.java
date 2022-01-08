package org.apache.iotdb.db.query.iterator;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.iterator.BaseTimeSeries;
import org.apache.iotdb.tsfile.read.query.iterator.LeafNode;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.Collections;

public class ServerSeriesIterator extends BaseTimeSeries {

  private final QueryContext context;
  private final RawDataQueryPlan queryPlan;
  private final LeafNode leafNode;

  public ServerSeriesIterator(QueryContext context, RawDataQueryPlan queryPlan, PartialPath path, Filter filter) {
    super(Collections.singletonList(path.getSeriesType()));
    this.context = context;
    this.queryPlan = queryPlan;

    IBatchReader reader = null;
    try {
      reader = generateNewBatchReader(path, filter);
    } catch (IOException e) {
      e.printStackTrace();
    }

    leafNode = new LeafNode(reader);
  }

  @Override
  public boolean hasNext() {
    try {
      return leafNode.hasNext();
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public Object[] next() {
    long ts = 0;
    try {
      ts = leafNode.next();
    } catch (IOException e) {
      return null;
    }
    Object value = leafNode.currentValue();
    return new Object[]{ts, value};
  }

  /**
   * Copied from {@link org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator}
   */

  protected Filter getTimeFilter(Filter filter) {
    if (filter instanceof UnaryFilter
        && ((UnaryFilter) filter).getFilterType() == FilterType.TIME_FILTER) {
      return filter;
    }
    if (filter instanceof AndFilter) {
      Filter leftTimeFilter = getTimeFilter(((AndFilter) filter).getLeft());
      Filter rightTimeFilter = getTimeFilter(((AndFilter) filter).getRight());
      if (leftTimeFilter != null && rightTimeFilter != null) {
        return filter;
      } else if (leftTimeFilter != null) {
        return leftTimeFilter;
      } else {
        return rightTimeFilter;
      }
    }
    return null;
  }

  protected IBatchReader generateNewBatchReader(PartialPath path, Filter filter)
      throws IOException {
    TSDataType dataType = path.getSeriesType();
    QueryDataSource queryDataSource;
    try {
      queryDataSource =
          QueryResourceManager.getInstance().getQueryDataSource(path, context, filter);
      // update valueFilter by TTL
      filter = queryDataSource.updateFilterUsingTTL(filter);
    } catch (Exception e) {
      throw new IOException(e);
    }

    // get the TimeFilter part in SingleSeriesExpression
    Filter timeFilter = getTimeFilter(filter);

    return new SeriesRawDataBatchReader(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        queryDataSource,
        timeFilter,
        filter,
        null,
        queryPlan.isAscending());
  }
}
