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

package org.apache.iotdb.confignode.procedure.impl.subscription;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.ConsumerGroupMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.runtime.TopicMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.subscription.OperateSubscriptionState;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractOperateSubscriptionProcedure
    extends AbstractNodeProcedure<OperateSubscriptionState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractOperateSubscriptionProcedure.class);

  private static final String SKIP_SUBSCRIPTION_PROCEDURE_MESSAGE =
      "Skip subscription-related operations and do nothing";

  private static final int RETRY_THRESHOLD = 1;

  // Only used in rollback to reduce the number of network calls
  // Pure in-memory object, not involved in snapshot serialization and deserialization.
  // TODO: consider serializing this variable later
  protected boolean isRollbackFromOperateOnDataNodesSuccessful = false;

  // Only used in rollback to avoid executing rollbackFromValidate multiple times
  // Pure in-memory object, not involved in snapshot serialization and deserialization.
  // TODO: consider serializing this variable later
  protected boolean isRollbackFromValidateSuccessful = false;

  protected AtomicReference<SubscriptionInfo> subscriptionInfo;

  protected AtomicReference<SubscriptionInfo> acquireLockInternal(
      ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return configNodeProcedureEnv
        .getConfigManager()
        .getSubscriptionManager()
        .getSubscriptionCoordinator()
        .lock();
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    LOGGER.info("ProcedureId {} try to acquire subscription lock.", getProcId());
    subscriptionInfo = acquireLockInternal(configNodeProcedureEnv);
    if (subscriptionInfo == null) {
      LOGGER.warn("ProcedureId {} failed to acquire subscription lock.", getProcId());
    } else {
      LOGGER.info("ProcedureId {} acquired subscription lock.", getProcId());
    }

    final ProcedureLockState procedureLockState = super.acquireLock(configNodeProcedureEnv);
    switch (procedureLockState) {
      case LOCK_ACQUIRED:
        if (subscriptionInfo == null) {
          LOGGER.warn(
              "ProcedureId {}: LOCK_ACQUIRED. The following procedure should not be executed without subscription lock.",
              getProcId());
        } else {
          LOGGER.info(
              "ProcedureId {}: LOCK_ACQUIRED. The following procedure should be executed with subscription lock.",
              getProcId());
        }
        break;
      case LOCK_EVENT_WAIT:
        if (subscriptionInfo == null) {
          LOGGER.warn(
              "ProcedureId {}: LOCK_EVENT_WAIT. Without acquiring subscription lock.", getProcId());
        } else {
          LOGGER.info(
              "ProcedureId {}: LOCK_EVENT_WAIT. Subscription lock will be released.", getProcId());
          configNodeProcedureEnv
              .getConfigManager()
              .getSubscriptionManager()
              .getSubscriptionCoordinator()
              .unlock();
          subscriptionInfo = null;
        }
        break;
      default:
        if (subscriptionInfo == null) {
          LOGGER.error(
              "ProcedureId {}: {}. Invalid lock state. Without acquiring subscription lock.",
              getProcId(),
              procedureLockState);
        } else {
          LOGGER.error(
              "ProcedureId {}: {}. Invalid lock state. Subscription lock will be released.",
              getProcId(),
              procedureLockState);
          configNodeProcedureEnv
              .getConfigManager()
              .getSubscriptionManager()
              .getSubscriptionCoordinator()
              .unlock();
          subscriptionInfo = null;
        }
        break;
    }
    return procedureLockState;
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    super.releaseLock(configNodeProcedureEnv);

    if (subscriptionInfo == null) {
      LOGGER.warn(
          "ProcedureId {} release lock. No need to release subscription lock.", getProcId());
    } else {
      LOGGER.info("ProcedureId {} release lock. Subscription lock will be released.", getProcId());
      if (this instanceof TopicMetaSyncProcedure
          || this instanceof ConsumerGroupMetaSyncProcedure) {
        LOGGER.info("Subscription meta sync procedure finished, updating last sync version.");
        configNodeProcedureEnv
            .getConfigManager()
            .getSubscriptionManager()
            .getSubscriptionCoordinator()
            .updateLastSyncedVersion();
      }
      configNodeProcedureEnv
          .getConfigManager()
          .getSubscriptionManager()
          .getSubscriptionCoordinator()
          .unlock();
      subscriptionInfo = null;
    }
  }

  protected abstract SubscriptionOperation getOperation();

  protected abstract boolean executeFromValidate(ConfigNodeProcedureEnv env)
      throws SubscriptionException;

  protected abstract void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException;

  protected abstract void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperateSubscriptionState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (subscriptionInfo == null) {
      LOGGER.warn(
          "ProcedureId {}: Subscription lock is not acquired, executeFromState({})'s execution will be skipped.",
          getProcId(),
          state);
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case VALIDATE:
          if (!executeFromValidate(env)) {
            LOGGER.info("ProcedureId {}: {}", getProcId(), SKIP_SUBSCRIPTION_PROCEDURE_MESSAGE);
            // On client side, the message returned after the successful execution of the
            // subscription command corresponding to this procedure is "Msg: The statement is
            // executed successfully."
            this.setResult(SKIP_SUBSCRIPTION_PROCEDURE_MESSAGE.getBytes(StandardCharsets.UTF_8));
            return Flow.NO_MORE_STATE;
          }
          setNextState(OperateSubscriptionState.OPERATE_ON_CONFIG_NODES);
          break;
        case OPERATE_ON_CONFIG_NODES:
          executeFromOperateOnConfigNodes(env);
          setNextState(OperateSubscriptionState.OPERATE_ON_DATA_NODES);
          break;
        case OPERATE_ON_DATA_NODES:
          executeFromOperateOnDataNodes(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Unknown state during executing operateSubscriptionProcedure, %s", state));
      }
    } catch (Exception e) {
      // Retry before rollback
      if (getCycles() < RETRY_THRESHOLD) {
        LOGGER.warn(
            "ProcedureId {}: Encountered error when trying to {} at state [{}], retry [{}/{}]",
            getProcId(),
            getOperation(),
            state,
            getCycles() + 1,
            RETRY_THRESHOLD,
            e);
        // Wait 3s for next retry
        TimeUnit.MILLISECONDS.sleep(3000L);
      } else {
        LOGGER.warn(
            "ProcedureId {}: All {} retries failed when trying to {} at state [{}], will rollback...",
            getProcId(),
            RETRY_THRESHOLD,
            getOperation(),
            state,
            e);
        setFailure(
            new ProcedureException(
                String.format(
                    "ProcedureId %s: Fail to %s because %s",
                    getProcId(), getOperation().name(), e.getMessage())));
      }
    }

    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperateSubscriptionState state)
      throws IOException, InterruptedException, ProcedureException {
    if (subscriptionInfo == null) {
      LOGGER.warn(
          "ProcedureId {}: Subscription lock is not acquired, rollbackState({})'s execution will be skipped.",
          getProcId(),
          state);
      return;
    }

    switch (state) {
      case VALIDATE:
        if (!isRollbackFromValidateSuccessful) {
          try {
            rollbackFromValidate(env);
            isRollbackFromValidateSuccessful = true;
          } catch (Exception e) {
            LOGGER.warn(
                "ProcedureId {}: Failed to rollback from state [{}], because {}",
                getProcId(),
                state,
                e.getMessage(),
                e);
          }
        }
        break;
      case OPERATE_ON_CONFIG_NODES:
        try {
          if (!isRollbackFromOperateOnDataNodesSuccessful) {
            rollbackFromOperateOnConfigNodes(env);
          }
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from state [{}], because {}",
              getProcId(),
              state,
              e.getMessage(),
              e);
        }
        break;
      case OPERATE_ON_DATA_NODES:
        try {
          rollbackFromOperateOnConfigNodes(env);
          rollbackFromOperateOnDataNodes(env);
          isRollbackFromOperateOnDataNodesSuccessful = true;
        } catch (Exception e) {
          LOGGER.warn(
              "ProcedureId {}: Failed to rollback from state [{}], because {}",
              getProcId(),
              state,
              e.getMessage(),
              e);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unknown state during rollback operateSubscriptionProcedure, %s", state));
    }
  }

  protected abstract void rollbackFromValidate(ConfigNodeProcedureEnv env);

  protected abstract void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException;

  protected abstract void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException;

  /**
   * Pushing all the topicMeta's to all the dataNodes.
   *
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing topic meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushTopicMetaResp> pushTopicMetaToDataNodes(ConfigNodeProcedureEnv env)
      throws IOException {
    final List<ByteBuffer> topicMetaBinaryList = new ArrayList<>();
    for (TopicMeta topicMeta : subscriptionInfo.get().getAllTopicMeta()) {
      topicMetaBinaryList.add(topicMeta.serialize());
    }

    return env.pushAllTopicMetaToDataNodes(topicMetaBinaryList);
  }

  public static boolean pushTopicMetaHasException(Map<Integer, TPushTopicMetaResp> respMap) {
    for (TPushTopicMetaResp resp : respMap.values()) {
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Pushing all the topicMeta's to all the dataNodes.
   *
   * @param env ConfigNodeProcedureEnv
   * @return The responseMap after pushing topic meta
   * @throws IOException Exception when Serializing to byte buffer
   */
  protected Map<Integer, TPushConsumerGroupMetaResp> pushConsumerGroupMetaToDataNodes(
      ConfigNodeProcedureEnv env) throws IOException {
    final List<ByteBuffer> consumerGroupMetaBinaryList = new ArrayList<>();
    for (ConsumerGroupMeta consumerGroupMeta : subscriptionInfo.get().getAllConsumerGroupMeta()) {
      consumerGroupMetaBinaryList.add(consumerGroupMeta.serialize());
    }

    return env.pushAllConsumerGroupMetaToDataNodes(consumerGroupMetaBinaryList);
  }

  public static boolean pushConsumerGroupMetaHasException(
      Map<Integer, TPushConsumerGroupMetaResp> respMap) {
    for (TPushConsumerGroupMetaResp resp : respMap.values()) {
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected OperateSubscriptionState getState(int stateId) {
    return OperateSubscriptionState.values()[stateId];
  }

  @Override
  protected int getStateId(OperateSubscriptionState state) {
    return state.ordinal();
  }

  @Override
  protected OperateSubscriptionState getInitialState() {
    return OperateSubscriptionState.VALIDATE;
  }
}
