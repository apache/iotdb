package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
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
      GroupByPlan groupByPlan, MetaGroupMember metaGroupMember)
      throws StorageEngineException, QueryProcessException {
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
  protected TimeGenerator getTimeGenerator(IExpression expression, QueryContext context,
      RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(expression, context, metaGroupMember, rawDataQueryPlan);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(Path path, RawDataQueryPlan dataQueryPlan,
      TSDataType dataType,
      QueryContext context,
      TsFileFilter fileFilter) throws StorageEngineException, QueryProcessException {
    return metaGroupMember.getReaderByTimestamp(path,
        dataQueryPlan.getAllSensorsInDevice(path.getDevice()), dataType, context);
  }
}
