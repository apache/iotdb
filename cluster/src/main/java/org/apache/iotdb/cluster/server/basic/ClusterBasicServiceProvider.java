package org.apache.iotdb.cluster.server.basic;

import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.ClusterSessionManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.basic.BasicServiceBaseProvider;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterBasicServiceProvider extends BasicServiceBaseProvider {
  private static final Logger logger = LoggerFactory.getLogger(ClusterBasicServiceProvider.class);

  /**
   * The Coordinator of the local node. Through this node queries data and meta from the cluster and
   * performs data manipulations to the cluster.
   */
  private Coordinator coordinator;

  public ClusterBasicServiceProvider() throws QueryProcessException {}

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /** Redirect the plan to the local Coordinator so that it will be processed cluster-wide. */
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      plan.checkIntegrity();
      if (!(plan instanceof SetSystemModePlan)
          && !(plan instanceof FlushPlan)
          && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
        throw new QueryProcessException(
            "Current system mode is read-only, does not support non-query operation");
      }
    } catch (QueryProcessException e) {
      logger.warn("Illegal plan detected： {}", plan);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }

    return coordinator.executeNonQueryPlan(plan);
  }

  /**
   * Generate and cache a QueryContext using "queryId". In the distributed version, the QueryContext
   * is a RemoteQueryContext.
   *
   * @return a RemoteQueryContext using queryId
   */
  @Override
  public QueryContext genQueryContext(
      long queryId, boolean debug, long startTime, String statement, long timeout) {
    RemoteQueryContext context =
        new RemoteQueryContext(queryId, debug, startTime, statement, timeout);
    ClusterSessionManager.getInstance().putContext(queryId, context);
    return context;
  }

  @Override
  public boolean executeNonQuery(PhysicalPlan plan) {
    TSStatus tsStatus = executeNonQueryPlan(plan);
    return tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }
}
