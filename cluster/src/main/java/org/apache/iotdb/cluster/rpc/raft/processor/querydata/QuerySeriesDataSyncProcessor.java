package org.apache.iotdb.cluster.rpc.raft.processor.querydata;

import com.alipay.remoting.BizContext;
import com.alipay.sofa.jraft.Status;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.manager.querynode.ClusterLocalQueryManager;
import org.apache.iotdb.cluster.rpc.raft.processor.BasicSyncUserProcessor;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.Stage;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.ProcessorException;

public class QuerySeriesDataSyncProcessor extends
    BasicSyncUserProcessor<QuerySeriesDataRequest> {

  @Override
  public Object handleRequest(BizContext bizContext, QuerySeriesDataRequest request)
      throws Exception {
    Stage stage = request.getStage();
    String groupId = request.getGroupID();
    PathType pathType = request.getPathType();
    QuerySeriesDataResponse response = new QuerySeriesDataResponse(groupId, pathType);
    switch (stage) {
      case INITIAL:
        handleNullRead(request.getReadConsistencyLevel(), groupId);
        ClusterLocalQueryManager.getInstance().createQueryDataSet(request, response);
        break;
      case READ_DATA:
        ClusterLocalQueryManager.getInstance().readBatchData(request, response);
        break;
      case CLOSE:
        ClusterLocalQueryManager.getInstance().close(request.getJobId());
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return response;
  }

  /**
   * It's necessary to do null read while creating query data set with a strong consistency level.
   *
   * @param readConsistencyLevel read concistency level
   * @param groupId group id
   */
  private void handleNullRead(int readConsistencyLevel, String groupId) throws ProcessorException {
    if (readConsistencyLevel == ClusterConstant.STRONG_CONSISTENCY_LEVEL) {
      Status nullReadTaskStatus = Status.OK();
      RaftUtils.handleNullReadToDataGroup(nullReadTaskStatus, groupId);
      if (!nullReadTaskStatus.isOk()) {
        throw new ProcessorException("Null read to data group failed");
      }
    }
  }

  @Override
  public String interest() {
    return QuerySeriesDataSyncProcessor.class.getName();
  }
}
