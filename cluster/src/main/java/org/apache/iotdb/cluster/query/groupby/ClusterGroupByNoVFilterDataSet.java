package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class ClusterGroupByNoVFilterDataSet extends GroupByWithoutValueFilterDataSet {

  private MetaGroupMember metaGroupMember;

  public ClusterGroupByNoVFilterDataSet(QueryContext context,
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
    this.metaGroupMember = metaGroupMember;

    initGroupBy(context, groupByPlan);
  }

  @Override
  protected GroupByExecutor getGroupByExecutor(Path path, TSDataType dataType, QueryContext context,
      Filter timeFilter, TsFileFilter fileFilter) {
    return new MergeGroupByExecutor(path, dataType, context, timeFilter, metaGroupMember);
  }
}
