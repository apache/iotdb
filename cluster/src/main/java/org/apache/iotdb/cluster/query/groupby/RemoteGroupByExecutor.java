package org.apache.iotdb.cluster.query.groupby;

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteGroupByExecutor implements GroupByExecutor {

  private static final Logger logger = LoggerFactory.getLogger(RemoteGroupByExecutor.class);

  private long executorId;
  private MetaGroupMember metaGroupMember;
  private Node source;
  private Node header;

  private List<AggregateResult> results = new ArrayList<>();
  private AtomicReference<List<ByteBuffer>> fetchResult = new AtomicReference<>();
  private GenericHandler<List<ByteBuffer>> handler;

  public RemoteGroupByExecutor(long executorId,
      MetaGroupMember metaGroupMember, Node source, Node header) {
    this.executorId = executorId;
    this.metaGroupMember = metaGroupMember;
    this.source = source;
    this.header = header;
    handler = new GenericHandler<>(source, fetchResult);
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  private void resetAggregateResults() {
    for (AggregateResult result : results) {
      result.reset();
    }
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException {
    DataClient client = metaGroupMember.getDataClient(source);
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        client.getGroupByResult(header, executorId, curStartTime, curEndTime, handler);
        fetchResult.wait(connectionTimeoutInMS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    List<ByteBuffer> aggrBuffers = fetchResult.get();
    resetAggregateResults();
    if (aggrBuffers != null) {
      for (int i = 0; i < aggrBuffers.size(); i++) {
        AggregateResult result = AggregateResult.deserializeFrom(aggrBuffers.get(i));
        results.get(i).merge(result);
      }
    }
    logger.debug("Fetched group by result from {} of [{}, {}]: {}", source, curStartTime,
        curEndTime, results);
    return results;
  }
}
