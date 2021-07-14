package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.UDTFAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.UDTFNonAlignDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

public class ClusterUDTFQueryExecutor extends ClusterDataQueryExecutor {
  protected final UDTFPlan udtfPlan;
  protected final MetaGroupMember metaGroupMember;

  public ClusterUDTFQueryExecutor(UDTFPlan udtfPlan, MetaGroupMember metaGroupMember) {
    super(udtfPlan, metaGroupMember);
    this.udtfPlan = udtfPlan;
    this.metaGroupMember = metaGroupMember;
  }

  public QueryDataSet executeWithoutValueFilterAlignByTime(QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException, InterruptedException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new UDTFAlignByTimeDataSet(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        readersOfSelectedSeries);
  }

  public QueryDataSet executeWithValueFilterAlignByTime(QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    TimeGenerator timestampGenerator = getTimeGenerator(context, udtfPlan);
    List<Boolean> cached =
        markFilterdPaths(
            udtfPlan.getExpression(),
            new ArrayList<>(udtfPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    List<IReaderByTimestamp> readersOfSelectedSeries =
        initSeriesReaderByTimestamp(context, udtfPlan, cached);
    return new UDTFAlignByTimeDataSet(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        readersOfSelectedSeries,
        cached);
  }

  public QueryDataSet executeWithoutValueFilterNonAlign(QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException, InterruptedException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new UDTFNonAlignDataSet(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        readersOfSelectedSeries);
  }

  public QueryDataSet executeWithValueFilterNonAlign(QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException {
    TimeGenerator timestampGenerator = getTimeGenerator(context, udtfPlan);
    List<Boolean> cached =
        markFilterdPaths(
            udtfPlan.getExpression(),
            new ArrayList<>(udtfPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    List<IReaderByTimestamp> readersOfSelectedSeries =
        initSeriesReaderByTimestamp(context, udtfPlan, cached);
    return new UDTFNonAlignDataSet(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        readersOfSelectedSeries,
        cached);
  }
}
