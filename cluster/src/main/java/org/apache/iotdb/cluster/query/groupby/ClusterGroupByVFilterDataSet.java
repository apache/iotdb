package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class ClusterGroupByVFilterDataSet extends GroupByWithValueFilterDataSet {

  private MetaGroupMember metaGroupMember;

  public ClusterGroupByVFilterDataSet(QueryContext context,
      GroupByPlan groupByPlan, MetaGroupMember metaGroupMember) throws StorageEngineException {
    this.paths = groupByPlan.getDeduplicatedPaths();
    this.dataTypes = groupByPlan.getDeduplicatedDataTypes();

    this.queryId = context.getQueryId();
    this.interval = groupByPlan.getInterval();
    this.slidingStep = groupByPlan.getSlidingStep();
    this.startTime = groupByPlan.getStartTime();
    this.endTime = groupByPlan.getEndTime();

    // init group by time partition
    this.usedIndex = 0;
    this.hasCachedTimeInterval = false;
    this.curEndTime = -1;

    this.timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    this.metaGroupMember = metaGroupMember;
    initGroupBy(context, groupByPlan);
  }

  @Override
  protected TimeGenerator getTimeGenerator(IExpression expression, QueryContext context)
      throws StorageEngineException {
    return new ClusterTimeGenerator(expression, context, metaGroupMember);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(Path path, TSDataType dataType, QueryContext context,
      TsFileFilter fileFilter) throws StorageEngineException {
    return metaGroupMember.getReaderByTimestamp(path, dataType, context);
  }
}
