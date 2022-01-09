package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

/**
 * Util to get Batch Readers for Paths or something...
 */
public class BatchReaderUtil {

  protected static IBatchReader getReaderInQuery(QueryContext context, SingleSeriesExpression expression, boolean ascending) {
    Filter valueFilter = expression.getFilter();
    PartialPath path = (PartialPath) expression.getSeriesPath();

    return getReaderInQuery(context, path, path.getSeriesType(), valueFilter, ascending);
  }

  protected static IBatchReader getReaderInQuery(QueryContext context, PartialPath path, TSDataType dataType, Filter valueFilter, boolean ascending) {

    QueryDataSource queryDataSource;
    MeasurementPath measurementPath;
    try {
      measurementPath = new MeasurementPath(path.getFullPath(), dataType);
      queryDataSource =
          QueryResourceManager.getInstance().getQueryDataSource(measurementPath, context, valueFilter);
      // update valueFilter by TTL
      valueFilter = queryDataSource.updateFilterUsingTTL(valueFilter);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    // get the TimeFilter part in SingleSeriesExpression
    Filter timeFilter = getTimeFilter(valueFilter);

    return new SeriesRawDataBatchReader(
        measurementPath,
        Collections.singleton(path.getMeasurement()), // JF: is this correct?
        dataType,
        context,
        queryDataSource,
        timeFilter,
        valueFilter,
        null,
        ascending);
  }

  protected static Filter getTimeFilter(Filter filter) {
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

}
