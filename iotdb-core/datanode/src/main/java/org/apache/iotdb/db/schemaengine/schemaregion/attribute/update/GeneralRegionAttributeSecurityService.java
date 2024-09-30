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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.dn.DnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.dn.DnToDnRequestType;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.mpp.rpc.thrift.TAttributeUpdateReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaRegionAttributeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class GeneralRegionAttributeSecurityService {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeneralRegionAttributeSecurityService.class);

  private final ExecutorService securityServiceExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.GENERAL_REGION_ATTRIBUTE_SECURITY_SERVICE.getName());

  private final Set<ISchemaRegion> regionLeaders = new HashSet<>();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private volatile boolean skipNext = false;

  public void startBroadcast(final ISchemaRegion schemaRegion) {
    if (regionLeaders.isEmpty()) {
      securityServiceExecutor.submit(this::execute);
      LOGGER.info("General region attribute security service is started successfully.");
    }

    regionLeaders.add(schemaRegion);
  }

  public void stopBroadcast(final ISchemaRegion schemaRegion) {
    regionLeaders.remove(schemaRegion);

    if (regionLeaders.isEmpty()) {
      securityServiceExecutor.shutdown();
      LOGGER.info("General region attribute security service is stopped successfully.");
    }
  }

  public void notifyBroadCast() {
    if (lock.tryLock()) {
      try {
        condition.signalAll();
      } finally {
        lock.unlock();
      }
    } else {
      skipNext = true;
    }
  }

  private void execute() {
    lock.lock();
    try {
      // All the "detailContainer"'s size will add up to at most "limit"
      // UpdateClearContainer and version / TEndPoint are not calculated
      final AtomicInteger limit =
          new AtomicInteger(
              CommonDescriptor.getInstance()
                  .getConfig()
                  .getPipeConnectorRequestSliceThresholdBytes());

      final Map<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>>
          attributeUpdateCommitMap = new HashMap<>();
      for (final ISchemaRegion regionLeader : regionLeaders) {
        final Pair<Long, Map<TDataNodeLocation, byte[]>> currentResult =
            regionLeader.getAttributeUpdateInfo(limit);
        if (currentResult.getRight().isEmpty()) {
          break;
        }
        attributeUpdateCommitMap.put(regionLeader.getSchemaRegionId(), currentResult);
      }

      // Send
      sendUpdateRequest(attributeUpdateCommitMap);

      if (!skipNext) {
        condition.await(
            IoTDBDescriptor.getInstance()
                .getConfig()
                .getGeneralRegionAttributeSecurityServiceIntervalSeconds(),
            TimeUnit.SECONDS);
      }
      skipNext = false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Interrupted when waiting for the next attribute broadcasting: {}", e.getMessage());
    } finally {
      lock.unlock();
      securityServiceExecutor.submit(this::execute);
    }
  }

  private void sendUpdateRequest(
      final Map<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>>
          attributeUpdateCommitMap) {
    final AsyncRequestContext<TAttributeUpdateReq, TSStatus, DnToDnRequestType, TDataNodeLocation>
        clientHandler = new AsyncRequestContext<>(DnToDnRequestType.UPDATE_ATTRIBUTE);

    attributeUpdateCommitMap.forEach(
        (id, pair) ->
            pair.getRight()
                .forEach(
                    (location, bytes) -> {
                      clientHandler.putNodeLocation(location.getDataNodeId(), location);
                      clientHandler
                          .putRequestIfAbsent(
                              location.getDataNodeId(), new TAttributeUpdateReq(new HashMap<>()))
                          .getAttributeUpdateMap()
                          .put(
                              id.getId(),
                              new TSchemaRegionAttributeInfo(
                                  pair.getLeft(), ByteBuffer.wrap(bytes)));
                    }));

    DnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithTimeoutInMs(
            clientHandler,
            IoTDBDescriptor.getInstance()
                .getConfig()
                .getGeneralRegionAttributeSecurityServiceTimeoutSeconds());

    final Set<Integer> failedDataNodes =
        clientHandler.getResponseMap().entrySet().stream()
            .filter(
                entry -> entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    for (final Iterator<Map.Entry<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>>> it =
            attributeUpdateCommitMap.entrySet().iterator();
        it.hasNext(); ) {
      final Map<TDataNodeLocation, byte[]> dataNodeLocationMap = it.next().getValue().getRight();
      dataNodeLocationMap
          .entrySet()
          .removeIf(
              locationEntry -> failedDataNodes.contains(locationEntry.getKey().getDataNodeId()));
      if (dataNodeLocationMap.isEmpty()) {
        it.remove();
      }
    }
  }

  /////////////////////////////// SingleTon ///////////////////////////////

  private GeneralRegionAttributeSecurityService() {
    // Do nothing
  }

  private static final class GeneralRegionAttributeSecurityServiceHolder {
    private static final GeneralRegionAttributeSecurityService INSTANCE =
        new GeneralRegionAttributeSecurityService();

    private GeneralRegionAttributeSecurityServiceHolder() {}
  }

  public static GeneralRegionAttributeSecurityService getInstance() {
    return GeneralRegionAttributeSecurityServiceHolder.INSTANCE;
  }
}
