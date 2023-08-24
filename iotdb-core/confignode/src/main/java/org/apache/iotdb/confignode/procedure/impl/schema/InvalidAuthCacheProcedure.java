package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.InvalidAuthCacheState;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InvalidAuthCacheProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, InvalidAuthCacheState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvalidAuthCacheProcedure.class);

  private PermissionManager permissionManager;

  private String user;
  private String role;

  private int timeoutMS;

  private static final int RETRY_THRESHOLD = 1;
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private List<Pair<TDataNodeConfiguration, Long>> dataNodesToInvalid;

  private Set<TDataNodeConfiguration> invalidedDNs;

  public InvalidAuthCacheProcedure() {
    super();
  }

  public InvalidAuthCacheProcedure(String user, String role, List<TDataNodeConfiguration> alldns) {
    super();
    this.user = user;
    this.role = role;
    this.dataNodesToInvalid = new ArrayList<>();
    for (TDataNodeConfiguration item : alldns) {
      this.dataNodesToInvalid.add(
          new Pair<TDataNodeConfiguration, Long>(item, System.currentTimeMillis()));
    }
    invalidedDNs = new HashSet<>();
    this.timeoutMS = commonConfig.getDatanodeTokenTimeoutMS();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, InvalidAuthCacheState state) {
    if (dataNodesToInvalid.isEmpty()) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          LOGGER.info("Start to invalid auth cache for " + "user: %s, role %s", user, role);
          // shall we need to check if the user/role has been deleted?
          if (dataNodesToInvalid.isEmpty()) {
            setNextState(InvalidAuthCacheState.DATANODE_AUTHCACHE_INVALID_DONE);
          }
          setNextState(InvalidAuthCacheState.DATANODE_AUTHCACHE_INVALIDING);
          break;
        case DATANODE_AUTHCACHE_INVALIDING:
          if (dataNodesToInvalid.isEmpty()) {
            setNextState(InvalidAuthCacheState.DATANODE_AUTHCACHE_INVALID_DONE);
          }
          TInvalidatePermissionCacheReq req = new TInvalidatePermissionCacheReq();
          TSStatus status;
          req.setUsername(user);
          req.setRoleName(role);
          for (Pair<TDataNodeConfiguration, Long> dataNodeInfo : dataNodesToInvalid) {
            if (dataNodeInfo.getRight() + this.timeoutMS < System.currentTimeMillis()) {
              invalidedDNs.add(dataNodeInfo.getLeft());
              dataNodesToInvalid.remove(dataNodeInfo);
              continue;
            }
            status =
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithRetry(
                        dataNodeInfo.getLeft().getLocation().getInternalEndPoint(),
                        req,
                        DataNodeRequestType.INVALIDATE_PERMISSION_CACHE);
            if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              invalidedDNs.add(dataNodeInfo.left);
              dataNodesToInvalid.remove(dataNodeInfo);
            }
          }
          if (dataNodesToInvalid.isEmpty()) {
            setNextState(InvalidAuthCacheState.DATANODE_AUTHCACHE_INVALID_DONE);
          } else {
            setNextState(InvalidAuthCacheState.DATANODE_AUTHCACHE_INVALIDING);
          }
          break;
        case DATANODE_AUTHCACHE_INVALID_DONE:
          LOGGER.info("finish invalid auth cache for user:%s, role %s", user, role);
          break;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in invalid auth cache", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to invalid auth cache :[user : %s, role : %s] in datanode",
            user, role, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to invalid auth cahce, user: %s, role: %s, datanode:%s",
                      user, role, dataNodesToInvalid.toString())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean isRollbackSupported(InvalidAuthCacheState state) {
    return false;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, InvalidAuthCacheState state) {
    // do nothing;
  }

  @Override
  protected InvalidAuthCacheState getState(int stateId) {
    return InvalidAuthCacheState.values()[stateId];
  }

  @Override
  protected int getStateId(InvalidAuthCacheState state) {
    return state.ordinal();
  }

  @Override
  protected InvalidAuthCacheState getInitialState() {
    return InvalidAuthCacheState.INIT;
  }
}
