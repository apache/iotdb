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
import org.apache.iotdb.consensus.pipe.client.manager.PipeConsensusAsyncClientManager;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.builder.IoTDBThriftAsyncPipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: 改造 handler，onComplete 的优化 + onComplete 加上出队逻辑
// TODO: 改造 batch 协议
// TODO: 改造 tsFile 传送协议
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

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final AtomicInteger alreadySentEventsInTransferBuffer = new AtomicInteger(0);

  private PipeConsensusSyncConnector retryConnector;

  private PipeConsensusAsyncClientManager asyncTransferClientManager;

  private IoTDBThriftAsyncPipeTransferBatchReqBuilder tabletBatchBuilder;

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // In PipeConsensus, one pipeConsensusTask corresponds to a pipeConsensusConnector. Thus,
    // `nodeUrls` here actually is a singletonList that contains one peer's TEndPoint. But here we
    // retain the implementation of list to cope with possible future expansion
    retryConnector = new PipeConsensusSyncConnector(nodeUrls);
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
      boolean result =
          transferBuffer.offer(
              event, COMMON_CONFIG.getPipeConsensusEventEnqueueTimeoutInMs(), TimeUnit.SECONDS);
      // add reference
      if (result) {
        ((EnrichedEvent) event).increaseReferenceCount(PipeConsensusAsyncConnector.class.getName());
      }
      return result;
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
      // decrease reference count
      ((EnrichedEvent) event)
          .decreaseReferenceCount(PipeConsensusAsyncConnector.class.getName(), true);
      // decrease alreadySentEventsCounts
      alreadySentEventsInTransferBuffer.decrementAndGet();
    }
  }

  /** handshake with all peers , Synchronized to avoid close connector when transfer event */
  @Override
  public synchronized void handshake() throws Exception {
    retryConnector.handshake();
  }

  @Override
  public void heartbeat() throws Exception {
    retryConnector.heartbeat();
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    boolean enqueueResult = addEvent2Buffer(tabletInsertionEvent);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }

    syncTransferQueuedEventsIfNecessary();
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    boolean enqueueResult = addEvent2Buffer(tsFileInsertionEvent);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }

    syncTransferQueuedEventsIfNecessary();
  }

  @Override
  public void transfer(Event event) throws Exception {
    boolean enqueueResult = addEvent2Buffer(event);
    if (!enqueueResult) {
      throw new PipeException(ENQUEUE_EXCEPTION_MSG);
    }

    syncTransferQueuedEventsIfNecessary();
  }

  /**
   * Transfer queued {@link Event}s which are waiting for retry.
   *
   * @throws Exception if an error occurs. The error will be handled by pipe framework, which will
   *     retry the {@link Event} and mark the {@link Event} as failure and stop the pipe if the
   *     retry times exceeds the threshold. TODO: pipe 框架对于 Consensus 改成无限重试，而不是超过次数后 停止
   */
  private synchronized void syncTransferQueuedEventsIfNecessary() throws Exception {
    while (!retryEventQueue.isEmpty()) {
      final Event peekedEvent = retryEventQueue.peek();
      // do transfer
      if (peekedEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        retryConnector.transfer((PipeInsertNodeTabletInsertionEvent) peekedEvent);
      } else if (peekedEvent instanceof PipeRawTabletInsertionEvent) {
        retryConnector.transfer((PipeRawTabletInsertionEvent) peekedEvent);
      } else if (peekedEvent instanceof PipeTsFileInsertionEvent) {
        retryConnector.transfer((PipeTsFileInsertionEvent) peekedEvent);
      } else {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "PipeConsensusAsyncConnector does not support transfer generic event: {}.",
              peekedEvent);
        }
      }
      // release resource
      if (peekedEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) peekedEvent)
            .decreaseReferenceCount(IoTDBDataRegionAsyncConnector.class.getName(), true);
      }

      final Event polledEvent = retryEventQueue.poll();
      if (polledEvent != peekedEvent) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error(
              "The event polled from the queue is not the same as the event peeked from the queue. "
                  + "Peeked event: {}, polled event: {}.",
              peekedEvent,
              polledEvent);
        }
      }
      if (polledEvent != null && LOGGER.isDebugEnabled()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Polled event {} from retry queue.", polledEvent);
        }
        // poll it from transferBuffer
        removeEventFromBuffer(polledEvent);
      }
    }
  }

  /**
   * Add failure event to retry queue.
   *
   * @param event event to retry
   */
  public synchronized void addFailureEventToRetryQueue(final Event event) {
    if (isClosed.get()) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncConnector.class.getName());
      }
      return;
    }

    retryEventQueue.offer(event);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Added event {} to retry queue.", event);
    }
  }

  /**
   * Add failure events to retry queue.
   *
   * @param events events to retry
   */
  public synchronized void addFailureEventsToRetryQueue(final Iterable<Event> events) {
    for (final Event event : events) {
      addFailureEventToRetryQueue(event);
    }
  }

  public synchronized void clearRetryEventsReferenceCount() {
    while (!retryEventQueue.isEmpty()) {
      final Event event = retryEventQueue.poll();
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncConnector.class.getName());
      }
    }
  }

  // synchronized to avoid close connector when transfer event
  @Override
  public synchronized void close() throws Exception {
    isClosed.set(true);

    retryConnector.close();
    clearRetryEventsReferenceCount();

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }
  }

  //////////////////////////// TODO: APIs provided for metric framework ////////////////////////////

}
