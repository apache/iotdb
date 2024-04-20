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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBConnector;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.builder.IoTDBThriftAsyncPipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.client.PipeConsensusAsyncClientManager;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.client.PipeConsensusSyncClientManager;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PipeConsensusAsyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusAsyncConnector.class);

  private static final String ENQUEUE_EXCEPTION_MSG =
      "Timeout: PipeConsensusConnector offers an event into transferBuffer failed, because transferBuffer is full";

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private final PriorityBlockingQueue<Event> retryEventQueue =
      new PriorityBlockingQueue<>(
          11,
          Comparator.comparing(
              e ->
                  // Non-enriched events will be put at the front of the queue,
                  // because they are more likely to be lost and need to be retried first.
                  e instanceof EnrichedEvent ? ((EnrichedEvent) e).getCommitId() : 0));

  private final BlockingQueue<Event> transferBuffer =
      new LinkedBlockingDeque<>(COMMON_CONFIG.getPipeConsensusEventBufferSize());

  private PipeConsensusSyncClientManager syncRetryAndHandshakeClientManager;

  private PipeConsensusAsyncClientManager asyncTransferClientManager;

  private IoTDBThriftAsyncPipeTransferBatchReqBuilder tabletBatchBuilder;

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    syncRetryAndHandshakeClientManager = PipeConsensusSyncClientManager.onPeers(nodeUrls);
    asyncTransferClientManager = PipeConsensusAsyncClientManager.getInstance();
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new IoTDBThriftAsyncPipeTransferBatchReqBuilder(parameters);
    }
  }

  /** Add an event to transferBuffer, whose events will be asynchronizedly transfer to receiver. */
  private boolean addEvent2Buffer(Event event) {
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus connector: one event enqueue, queue size = {}, limit size = {}",
            transferBuffer.size(),
            COMMON_CONFIG.getPipeConsensusEventBufferSize());
      }
      return transferBuffer.offer(
          event, COMMON_CONFIG.getPipeConsensusEventEnqueueTimeoutInMs(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.info("PipeConsensusConnector transferBuffer queue offer is interrupted.", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * if one event is successfully processed by receiver in PipeConsensus, we will remove this event
   * from transferBuffer in order to transfer other event.
   */
  public synchronized void removeEventFromBuffer(Event event) {
    synchronized (this) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "PipeConsensus connector: one event removed from queue, queue size = {}, limit size = {}",
            transferBuffer.size(),
            COMMON_CONFIG.getPipeConsensusEventBufferSize());
      }
      Iterator<Event> iterator = transferBuffer.iterator();
      Event current = iterator.next();
      while (!current.equals(event) && iterator.hasNext()) {
        current = iterator.next();
      }
      iterator.remove();
    }
  }

  /** handshake with all peers */
  @Override
  public void handshake() throws Exception {
    syncRetryAndHandshakeClientManager.checkClientStatusAndTryReconstructIfNecessary(nodeUrls);
  }

  @Override
  public void heartbeat() throws Exception {
    try {
      handshake();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
      throw e;
    }
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    boolean enqueueResult = addEvent2Buffer(tabletInsertionEvent);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }
    // TODO: 向集群所有副本 transfer
    // TODO：改造 request，加上 commitId 和 rebootTimes
    // TODO: 改造 handler，onComplete 的优化 + onComplete 加上出队逻辑

  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    boolean enqueueResult = addEvent2Buffer(tsFileInsertionEvent);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }
    super.transfer(tsFileInsertionEvent);
  }

  @Override
  public void transfer(Event event) throws Exception {
    boolean enqueueResult = addEvent2Buffer(event);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }
  }

  // synchronized to avoid close connector when transfer event
  @Override
  public synchronized void close() throws Exception {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }
  }

  //////////////////////////// TODO: APIs provided for metric framework ////////////////////////////

}
