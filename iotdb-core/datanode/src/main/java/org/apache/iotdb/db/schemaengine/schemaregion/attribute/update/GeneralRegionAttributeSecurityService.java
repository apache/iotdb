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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.client.dn.DnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.dn.DnToDnRequestType;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeCommitUpdateNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.impl.SchemaRegionMemoryImpl;
import org.apache.iotdb.mpp.rpc.thrift.TAttributeUpdateReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaRegionAttributeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class GeneralRegionAttributeSecurityService implements IService {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeneralRegionAttributeSecurityService.class);

  private static final IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();

  private final ExecutorService securityServiceExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.GENERAL_REGION_ATTRIBUTE_SECURITY_SERVICE.getName());

  private final Map<Integer, Pair<Long, Integer>> dataNodeId2FailureDurationAndTimesMap =
      new HashMap<>();
  private final Set<ISchemaRegion> regionLeaders =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Map<SchemaRegionId, String> regionId2DatabaseMap = new ConcurrentHashMap<>();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private volatile boolean skipNextSleep = false;
  private volatile boolean allowSubmitListen = false;

  public void startBroadcast(final ISchemaRegion schemaRegion) {
    if (schemaRegion instanceof SchemaRegionMemoryImpl) {
      regionId2DatabaseMap.put(
          schemaRegion.getSchemaRegionId(), schemaRegion.getDatabaseFullPath().substring(5));
      regionLeaders.add(schemaRegion);
    }
  }

  public void stopBroadcast(final ISchemaRegion schemaRegion) {
    regionLeaders.remove(schemaRegion);
    // Reserve the database info for concurrency simplicity
  }

  public void notifyBroadCast() {
    if (lock.tryLock()) {
      try {
        condition.signalAll();
      } finally {
        lock.unlock();
      }
    } else {
      skipNextSleep = true;
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

      final AtomicBoolean hasRemaining = new AtomicBoolean(false);
      final Map<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>>
          attributeUpdateCommitMap = new HashMap<>();
      for (final ISchemaRegion regionLeader : regionLeaders) {
        final Pair<Long, Map<TDataNodeLocation, byte[]>> currentResult =
            regionLeader.getAttributeUpdateInfo(limit, hasRemaining);
        if (currentResult.getRight().isEmpty()) {
          break;
        }
        attributeUpdateCommitMap.put(regionLeader.getSchemaRegionId(), currentResult);
      }

      if (hasRemaining.get()) {
        skipNextSleep = true;
      }

      if (!attributeUpdateCommitMap.isEmpty()) {
        // Send & may shrink
        final Map<SchemaRegionId, Set<TDataNodeLocation>> shrinkMap =
            sendUpdateRequestAndMayShrink(attributeUpdateCommitMap);

        // Commit
        attributeUpdateCommitMap.forEach(
            (schemaRegionId, pair) -> {
              if (!new RegionWriteExecutor()
                  .execute(
                      schemaRegionId,
                      new TableDeviceAttributeCommitUpdateNode(
                          new PlanNodeId(""),
                          pair.getLeft(),
                          pair.getRight(),
                          shrinkMap.getOrDefault(schemaRegionId, Collections.emptySet()),
                          new TDataNodeLocation(
                              iotdbConfig.getDataNodeId(),
                              null,
                              new TEndPoint(
                                  iotdbConfig.getInternalAddress(), iotdbConfig.getInternalPort()),
                              null,
                              null,
                              null)))
                  .isAccepted()) {
                // May fail due to region shutdown, migration or other reasons
                // Just ignore
                skipNextSleep = false;
                LOGGER.warn(
                    "Failed to write attribute commit message to region {}.", schemaRegionId);
              }
            });
      }

      if (!skipNextSleep) {
        condition.await(
            iotdbConfig.getGeneralRegionAttributeSecurityServiceIntervalSeconds(),
            TimeUnit.SECONDS);
      }
      skipNextSleep = false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Interrupted when waiting for the next attribute broadcasting: {}", e.getMessage());
    } finally {
      lock.unlock();

      if (allowSubmitListen) {
        securityServiceExecutor.submit(this::execute);
      }
    }
  }

  // Shrink the locations which do not exist in cluster dataNodes
  private @Nonnull Map<SchemaRegionId, Set<TDataNodeLocation>> detectNodeShrinkage(
      final Map<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>>
          attributeUpdateCommitMap) {
    final TShowClusterResp showClusterResp;
    try (final ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      showClusterResp = client.showCluster();
    } catch (final ClientManagerException | TException e) {
      LOGGER.warn("Failed to fetch dataNodeLocations, will retry.");
      return Collections.emptyMap();
    }
    dataNodeId2FailureDurationAndTimesMap.clear();
    final Set<TDataNodeLocation> realDataNodeLocations = new HashSet<>();
    showClusterResp
        .getDataNodeList()
        .forEach(
            location -> {
              location.setDataRegionConsensusEndPoint(null);
              location.setMPPDataExchangeEndPoint(null);
              location.setSchemaRegionConsensusEndPoint(null);
              location.setClientRpcEndPoint(null);
              realDataNodeLocations.add(location);
            });
    return attributeUpdateCommitMap.entrySet().stream()
        .filter(
            entry -> {
              entry.getValue().getRight().keySet().removeIf(realDataNodeLocations::contains);
              return !entry.getValue().getRight().isEmpty();
            })
        .collect(
            Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRight().keySet()));
  }

  private @Nonnull Map<SchemaRegionId, Set<TDataNodeLocation>> sendUpdateRequestAndMayShrink(
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
                      clientHandler.putRequestIfAbsent(
                          location.getDataNodeId(), new TAttributeUpdateReq(new HashMap<>()));
                      clientHandler
                          .getRequest(location.getDataNodeId())
                          .getAttributeUpdateMap()
                          .put(
                              id.getId(),
                              new TSchemaRegionAttributeInfo(
                                  pair.getLeft(),
                                  regionId2DatabaseMap.get(id),
                                  ByteBuffer.wrap(bytes)));
                    }));

    DnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithTimeoutInMs(
            clientHandler,
            IoTDBDescriptor.getInstance()
                .getConfig()
                .getGeneralRegionAttributeSecurityServiceTimeoutSeconds());

    final AtomicBoolean needFetch = new AtomicBoolean(false);

    final Set<Integer> failedDataNodes =
        clientHandler.getResponseMap().entrySet().stream()
            .filter(
                entry -> {
                  final boolean failed =
                      entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode();
                  if (failed) {
                    dataNodeId2FailureDurationAndTimesMap.compute(
                        entry.getKey(),
                        (k, v) -> {
                          if (Objects.isNull(v)) {
                            return new Pair<>(System.currentTimeMillis(), 1);
                          }
                          v.setRight(v.getRight() + 1);
                          if (System.currentTimeMillis() - v.getLeft()
                                  >= iotdbConfig
                                      .getGeneralRegionAttributeSecurityServiceFailureDurationSecondsToFetch()
                              || v.getRight()
                                  >= iotdbConfig
                                      .getGeneralRegionAttributeSecurityServiceFailureTimesToFetch()) {
                            needFetch.set(true);
                          }
                          return v;
                        });
                  } else {
                    dataNodeId2FailureDurationAndTimesMap.remove(entry.getKey());
                  }
                  return failed;
                })
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

    // Compute node shrinkage before failure removal in commit
    final Map<SchemaRegionId, Set<TDataNodeLocation>> result =
        needFetch.get() ? detectNodeShrinkage(attributeUpdateCommitMap) : Collections.emptyMap();

    for (final Iterator<Map.Entry<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>>> it =
            attributeUpdateCommitMap.entrySet().iterator();
        it.hasNext(); ) {
      final Map.Entry<SchemaRegionId, Pair<Long, Map<TDataNodeLocation, byte[]>>> currentEntry =
          it.next();
      final Map<TDataNodeLocation, byte[]> dataNodeLocationMap = currentEntry.getValue().getRight();
      dataNodeLocationMap
          .entrySet()
          .removeIf(
              locationEntry -> failedDataNodes.contains(locationEntry.getKey().getDataNodeId()));
      // Reserve the schemaIds with shrinkage for commit convenience
      if (dataNodeLocationMap.isEmpty() && !result.containsKey(currentEntry.getKey())) {
        it.remove();
      }
    }
    return result;
  }

  /////////////////////////////// IService ///////////////////////////////

  @Override
  public void start() throws StartupException {
    allowSubmitListen = true;
    securityServiceExecutor.submit(this::execute);

    LOGGER.info("General region attribute security service is started successfully.");
  }

  @Override
  public void stop() {
    allowSubmitListen = false;
    securityServiceExecutor.shutdown();

    LOGGER.info("General region attribute security service is stopped successfully.");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.GENERAL_REGION_ATTRIBUTE_SECURITY_SERVICE;
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
