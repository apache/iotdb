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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.thrift.TException;

public class RemoteGroupByExecutor implements GroupByExecutor {

  private long executorId;
  private MetaGroupMember metaGroupMember;
  private Node source;
  private Node header;

  private List<Pair<AggregateResult, Integer>> results = new ArrayList<>();
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
  public void addAggregateResult(AggregateResult aggrResult, int index) {
    results.add(new Pair<>(aggrResult, index));
  }

  @Override
  public void resetAggregateResults() {
    for (Pair<AggregateResult, Integer> result : results) {
      result.left.reset();
    }
  }

  @Override
  public List<Pair<AggregateResult, Integer>> calcResult(long curStartTime, long curEndTime)
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
    if (aggrBuffers != null) {
      for (int i = 0; i < aggrBuffers.size(); i++) {
        AggregateResult result = AggregateResult.deserializeFrom(aggrBuffers.get(i));
        results.get(i).left.merge(result);
      }
    }
    return results;
  }
}
