package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillWithValueFilterDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class ClusterGroupByFillWithValueFilterDataSet extends GroupByFillWithValueFilterDataSet {

  private MetaGroupMember metaGroupMember;
  private ClusterReaderFactory readerFactory;

  public ClusterGroupByFillWithValueFilterDataSet(
      QueryContext context,
      GroupByTimeFillPlan groupByTimeFillPlan,
      MetaGroupMember metaGroupMember)
      throws StorageEngineException, QueryProcessException {
    super(context, groupByTimeFillPlan);

    this.metaGroupMember = metaGroupMember;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(context, metaGroupMember, rawDataQueryPlan, false);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan dataQueryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return readerFactory.getReaderByTimestamp(
        path,
        dataQueryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        dataQueryPlan.isAscending(),
        null);
  }
}
