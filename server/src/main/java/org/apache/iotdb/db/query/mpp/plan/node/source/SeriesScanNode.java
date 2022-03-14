package org.apache.iotdb.db.query.mpp.plan.node.source;

import org.apache.iotdb.db.query.mpp.common.OrderBy;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * SeriesScanOperator is responsible for read data a specific series. When reading data, the SeriesScanOperator
 * can read the raw data batch by batch. And also, it can leverage the filter and other info to decrease the
 * result set.
 *
 * <p>Children type: no child is allowed for SeriesScanNode
 */
public class SeriesScanNode extends SourceNode {

  // The path of the target series which will be scanned.
  private Path seriesPath;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private OrderBy scanOrder = OrderBy.TIMESTAMP_ASC;

  // Filter data in current series.
  private Filter filter;

  // Limit for result set. The default value is -1, which means no limit
  private int limit;

  // offset for result set. The default value is 0
  private int offset;

  public SeriesScanNode(PlanNodeId id, Path seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void open() throws Exception {}

  public void setScanOrder(OrderBy scanOrder) {
    this.scanOrder = scanOrder;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
}
