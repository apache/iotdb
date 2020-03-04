package org.apache.iotdb.cluster.query;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.FillQueryExecutor;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class ClusterFillExecutor extends FillQueryExecutor {

  private MetaGroupMember metaGroupMember;

  public ClusterFillExecutor(List<Path> selectedSeries,
      List<TSDataType> dataTypes,
      long queryTime,
      Map<TSDataType, IFill> typeIFillMap,
      MetaGroupMember metaGroupMember) {
    super(selectedSeries, dataTypes, queryTime, typeIFillMap);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected void configureFill(IFill fill, TSDataType dataType, Path path, QueryContext context,
      long queryTime) throws StorageEngineException {
    fill.setDataType(dataType);
    fill.setQueryTime(queryTime);
    fill.setAllDataReader(metaGroupMember.getSeriesReader(path, dataType, fill.getFilter(), null,
     context));
  }
}
