/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.InvalidAuthCacheState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
            break;
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
          Iterator<Pair<TDataNodeConfiguration, Long>> it = dataNodesToInvalid.iterator();
          while (it.hasNext()) {
            if (it.next().getRight() + this.timeoutMS < System.currentTimeMillis()) {
              invalidedDNs.add(it.next().getLeft());
              it.remove();
              continue;
            }
            status =
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithRetry(
                        it.next().getLeft().getLocation().getInternalEndPoint(),
                        req,
                        DataNodeRequestType.INVALIDATE_PERMISSION_CACHE);
            if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              invalidedDNs.add(it.next().getLeft());
              it.remove();
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
          return Flow.NO_MORE_STATE;
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

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.INVALID_DATANODE_AUTH_CACHE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(user, stream);
    ReadWriteIOUtils.write(role, stream);
    ReadWriteIOUtils.write(dataNodesToInvalid.size(), stream);
    for (Pair<TDataNodeConfiguration, Long> item : dataNodesToInvalid) {
      ThriftCommonsSerDeUtils.serializeTDataNodeConfiguration(item.getLeft(), stream);
      ReadWriteIOUtils.write(item.getRight(), stream);
    }
    ReadWriteIOUtils.write(invalidedDNs.size(), stream);
    for (TDataNodeConfiguration item : invalidedDNs) {
      ThriftCommonsSerDeUtils.serializeTDataNodeConfiguration(item, stream);
    }
    ReadWriteIOUtils.write(timeoutMS, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.user = ReadWriteIOUtils.readString(byteBuffer);
    this.role = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    this.dataNodesToInvalid = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      TDataNodeConfiguration datanode =
          ThriftCommonsSerDeUtils.deserializeTDataNodeConfiguration(byteBuffer);
      Long timestamp = ReadWriteIOUtils.readLong(byteBuffer);
      this.dataNodesToInvalid.add(new Pair<TDataNodeConfiguration, Long>(datanode, timestamp));
    }
    this.invalidedDNs = new HashSet<>();
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      this.invalidedDNs.add(ThriftCommonsSerDeUtils.deserializeTDataNodeConfiguration(byteBuffer));
    }
    this.timeoutMS = ReadWriteIOUtils.readInt(byteBuffer);
  }

  @TestOnly
  public void removeAllDNS() {
    Iterator<Pair<TDataNodeConfiguration, Long>> it = dataNodesToInvalid.iterator();
    while (it.hasNext()) {
      invalidedDNs.add(it.next().getLeft());
      it.remove();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InvalidAuthCacheProcedure that = (InvalidAuthCacheProcedure) o;
    return user.equals(that.user)
        && role.equals(that.role)
        && Objects.equals(dataNodesToInvalid, ((InvalidAuthCacheProcedure) o).dataNodesToInvalid)
        && Objects.equals(invalidedDNs, that.invalidedDNs);
  }
}
