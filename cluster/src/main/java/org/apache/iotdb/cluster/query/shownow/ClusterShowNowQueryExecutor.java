package org.apache.iotdb.cluster.query.shownow;

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.sys.ShowNowPlan;
import org.apache.iotdb.db.qp.utils.ShowNowUtils;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowNowDataSet;
import org.apache.iotdb.db.query.dataset.ShowNowResult;
import org.apache.iotdb.db.query.executor.ShowNowQueryExecutor;
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
import java.util.concurrent.*;

import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.*;

public class ClusterShowNowQueryExecutor extends ShowNowQueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(ClusterShowNowQueryExecutor.class);
  private MetaGroupMember metaGroupMember;

  private static ExecutorService showNowQueryPool =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  public ClusterShowNowQueryExecutor(ShowNowPlan showNowPlan, MetaGroupMember metaGroupMember) {
    super();
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public QueryDataSet execute(QueryContext context, ShowNowPlan showNowPlan)
      throws MetadataException {
    ConcurrentSkipListSet<ShowNowResult> resultSet = new ConcurrentSkipListSet<>();

    ExecutorService pool =
        new ThreadPoolExecutor(
            THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    List<Node> nodeList = metaGroupMember.getPartitionTable().getAllNodes();
    List<Future<Void>> futureList = new ArrayList<>();
    for (Node node : nodeList) {
      futureList.add(
          pool.submit(
              () -> {
                try {
                  showNow(node, showNowPlan, resultSet, context);
                } catch (Exception e) {
                  logger.error("Cannot get show now result of {} from {}", showNowPlan, node);
                }
                return null;
              }));
    }

    waitForThreadPool(futureList, pool, "showNow()");
    List<ShowNowResult> showNowResults = applyShowNow(resultSet);
    return new ShowNowDataSet(showNowResults, context, showNowPlan);
  }

  private void showNow(
      Node node, ShowNowPlan showNowPlan, Set<ShowNowResult> resultSet, QueryContext context) {
    if (node.equals(metaGroupMember.getThisNode())) {
      showLocalNow(node, showNowPlan, resultSet, context);
    } else {
      showRemoteNow(node, showNowPlan, resultSet);
    }
  }

  private void showLocalNow(
      Node node, ShowNowPlan plan, Set<ShowNowResult> resultSet, QueryContext context) {
    try {
      List<ShowNowResult> localResult = null;
      try {
        localResult = new ShowNowUtils().getShowNowResults();
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("ClusterShowNowQueryExecutor showLocalNow");
      }
      resultSet.addAll(localResult);
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
      logger.error("Failed to execute show now {} in node: {}.", plan, node);
    }
  }

  private ByteBuffer showNow(Node node, ShowNowPlan plan)
      throws IOException, TException, InterruptedException {
    ByteBuffer resultBinary;

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      resultBinary = SyncClientAdaptor.getAllShowNow(client, metaGroupMember.getHeader(), plan);
    } else {
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
        SyncDataClient syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        plan.serialize(dataOutputStream);
        try {
          resultBinary = syncDataClient.getShowNow(metaGroupMember.getHeader());
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
