package org.apache.iotdb.cluster.query.shownow;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.query.last.ClusterLastQueryExecutor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.sys.ShowNowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowNowDataSet;
import org.apache.iotdb.db.query.dataset.ShowNowResult;
import org.apache.iotdb.db.query.executor.ShowNowQueryExecutor;
import org.apache.iotdb.db.utils.ShowNowUtils;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.LOG_FAIL_CONNECT;
import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.THREAD_POOL_SIZE;
import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.waitForThreadPool;

public class ClusterShowNowQueryExecutor extends ShowNowQueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(ClusterLastQueryExecutor.class);
  private MetaGroupMember metaGroupMember;

  private static ExecutorService showNowQueryPool =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  public ClusterShowNowQueryExecutor(ShowNowPlan showNowPlan, MetaGroupMember metaGroupMember) {
    super();
    this.metaGroupMember = metaGroupMember;
  }
  //
  //  class ShowNowTask implements Callable<List<ShowNowResult>> {
  //    private Node node;
  //    private QueryContext context;
  //    private ShowNowPlan showNowPlan;
  //
  //    ShowNowTask(Node node, QueryContext context, ShowNowPlan showNowPlan) {
  //      this.node = node;
  //      this.context = context;
  //      this.showNowPlan = showNowPlan;
  //    }
  //
  //    @Override
  //    public List<ShowNowResult> call() throws Exception {
  //      return null;
  //    }
  //
  //    private List<ShowNowResult> showNow(QueryContext context, ShowNowPlan showNowPlan) {
  //      List<Node> nodeList = metaGroupMember.getPartitionTable().getAllNodes();
  //      for (Node node : nodeList) {
  //        if (node.equals(metaGroupMember.getThisNode())) {
  //          showNowLocal(node, context, showNowPlan);
  //        } else {
  //          showNowRemote(node,context,showNowPlan);
  //        }
  //      }
  //    }
  //
  //    private List<ShowNowResult> showNowLocal(
  //        Node node, QueryContext context, ShowNowPlan showNowPlan) {
  //      return new ShowNowUtils().getShowNowResults();
  //    }
  //
  //    private List<ShowNowResult> showNowRemote(Node node,QueryContext context, ShowNowPlan
  // showNowPlan){
  //      try {
  //        ByteBuffer buffer;
  //        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
  //          buffer = lastAsync(node, context);
  //        } else {
  //          buffer = lastSync(node, context);
  //        }
  //        if (buffer == null) {
  //          continue;
  //        }
  //
  //        List<TimeValuePair> timeValuePairs = new ArrayList<>();
  //        for (int i = 0; i < seriesPaths.size(); i++) {
  //          timeValuePairs.add(SerializeUtils.deserializeTVPair(buffer));
  //        }
  //        List<Pair<Boolean, TimeValuePair>> results = new ArrayList<>();
  //        for (int i = 0; i < seriesPaths.size(); i++) {
  //          TimeValuePair pair = timeValuePairs.get(i);
  //          results.add(new Pair<>(true, pair));
  //        }
  //        return results;
  //      } catch (TException e) {
  //        logger.warn("Query last of {} from {} errored", group, seriesPaths, e);
  //        return Collections.emptyList();
  //      } catch (InterruptedException e) {
  //        Thread.currentThread().interrupt();
  //        logger.warn("Query last of {} from {} interrupted", group, seriesPaths, e);
  //        return Collections.emptyList();
  //      } catch (IOException e) {
  //        e.printStackTrace();
  //      }
  //    }
  //
  //    private ByteBuffer lastAsync(Node node, QueryContext context)
  //            throws TException, InterruptedException, IOException {
  //      ByteBuffer buffer;
  //      AsyncDataClient asyncDataClient;
  //      try {
  //        asyncDataClient =
  //                metaGroupMember
  //                        .getClientProvider()
  //                        .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
  //      } catch (IOException e) {
  //        logger.warn("can not get client for node= {}", node);
  //        return null;
  //      }
  //      buffer = SyncClientAdaptor.getAllShowNow(asyncDataClient,node,showNowPlan);
  //      return buffer;
  //    }
  //    private ByteBuffer lastSync(Node node, QueryContext context) throws TException {
  //      try (SyncDataClient client =
  //                   metaGroupMember
  //                           .getClientProvider()
  //                           .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
  //        return client.getShowNow(node,)
  //      } catch (IOException e) {
  //        logger.warn("can not get client for node= {}", node);
  //        return null;
  //      }
  //    }
  //
  //  }

  @Override
  public QueryDataSet execute(QueryContext context, ShowNowPlan showNowPlan)
      throws MetadataException {
    ConcurrentSkipListSet<ShowNowResult> resultSet = new ConcurrentSkipListSet<>();
    ExecutorService pool =
        new ThreadPoolExecutor(
            THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    List<Node> nodeList = metaGroupMember.getPartitionTable().getAllNodes();
    List<Future<Void>> futureList = new ArrayList<>();
    for (Node node : nodeList) {
      futureList.add(
          pool.submit(
              () -> {
                try {
                  showNow(node, showNowPlan, resultSet, context);
                  // showNow(group, plan, resultSet, context);
                } catch (Exception e) {
                  logger.error(
                      "****************Cannot get show now result of {} from {}",
                      showNowPlan,
                      node);
                }
                return null;
              }));
    }
    waitForThreadPool(futureList, pool, "showNow()");
    List<ShowNowResult> showNowResults = applyShowNow(resultSet);
    return new ShowNowDataSet(showNowResults, context, showNowPlan);
  }

  private void showNow(
      Node node, ShowNowPlan showNowPlan, Set<ShowNowResult> resultSet, QueryContext context)
      throws MetadataException, CheckConsistencyException {
    //    if (node.equals(metaGroupMember.getThisNode())) {
    //      showLocalNow(node, showNowPlan, resultSet, context);
    //    } else {
    //      showRemoteNow(node, showNowPlan, resultSet);
    //    }
    showRemoteNow(node, showNowPlan, resultSet);
  }

  private void showLocalNow(
      Node node, ShowNowPlan plan, Set<ShowNowResult> resultSet, QueryContext context) {
    //    Node header = group.getHeader();
    //    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    //    localDataMember.syncLeaderWithConsistencyCheck(false);
    try {
      List<ShowNowResult> localResult = null;
      try {
        localResult = new ShowNowUtils().getShowNowResults();
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("********CMManager showLocalNow");
      }
      try {
        if (localResult != null) resultSet.addAll(localResult);
      } catch (Exception e) {
        logger.error("******************** showLocalNow 1665");
        e.printStackTrace();
      }
      logger.debug("Fetched local now {} schemas from {}", localResult.size(), node);
    } catch (Exception e) {
      logger.error("Cannot execute show now plan  {} from {} locally.", plan, node);
      throw e;
    }
  }

  private void showRemoteNow(Node node, ShowNowPlan plan, Set<ShowNowResult> resultSet) {
    ByteBuffer resultBinary = null;
    try {
      resultBinary = showNow(node, plan);
    } catch (IOException e) {
      logger.error(LOG_FAIL_CONNECT, node, e);
    } catch (TException e) {
      logger.error("Error occurs when getting  now in node {}.", node, e);
    } catch (InterruptedException e) {
      logger.error("Interrupted when getting  now in node {}.", node, e);
      Thread.currentThread().interrupt();
    }

    if (resultBinary != null) {
      int size = resultBinary.getInt();
      logger.debug("Fetched remote now {} schemas  from {}", size, node);
      for (int i = 0; i < size; i++) {
        resultSet.add(ShowNowResult.deserialize(resultBinary));
      }
    } else {
      logger.error("Failed to execute show now {} in group: {}.", plan, node);
    }
  }

  private ByteBuffer showNow(Node node, ShowNowPlan plan)
      throws IOException, TException, InterruptedException {
    ByteBuffer resultBinary;

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          metaGroupMember
              .getClientProvider()
              .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
      resultBinary = SyncClientAdaptor.getAllShowNow(client, node, plan);
    } else {
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
          SyncDataClient syncDataClient =
              metaGroupMember
                  .getClientProvider()
                  .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
        plan.serialize(dataOutputStream);
        try {
          resultBinary =
              syncDataClient.getShowNow(node, ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
        } catch (TException e) {
          // the connection may be broken, close it to avoid it being reused
          syncDataClient.getInputProtocol().getTransport().close();
          throw e;
        }
      }
    }
    return resultBinary;
  }

  private List<ShowNowResult> applyShowNow(ConcurrentSkipListSet<ShowNowResult> resultSet) {
    List<ShowNowResult> showNowResults = new ArrayList<>();
    Iterator<ShowNowResult> iterator = resultSet.iterator();
    while (iterator.hasNext()) {
      showNowResults.add(iterator.next());
    }
    return showNowResults;
  }
}
