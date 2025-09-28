1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
1import org.apache.iotdb.commons.conf.CommonConfig;
1import org.apache.iotdb.commons.conf.CommonDescriptor;
1import org.apache.iotdb.commons.utils.PathUtils;
1import org.apache.iotdb.confignode.consensus.request.write.partition.AutoCleanPartitionTablePlan;
1import org.apache.iotdb.confignode.manager.ConfigManager;
1import org.apache.iotdb.consensus.exception.ConsensusException;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.List;
1import java.util.Map;
1import java.util.TreeMap;
1
1import static org.apache.iotdb.confignode.manager.partition.PartitionManager.CONSENSUS_WRITE_ERROR;
1
1/** A cleaner that automatically deletes the expired mapping within the partition table. */
1public class PartitionTableAutoCleaner<Env> extends InternalProcedure<Env> {
1
1  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionTableAutoCleaner.class);
1
1  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
1
1  private static final String timestampPrecision =
1      CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
1
1  private final ConfigManager configManager;
1
1  public PartitionTableAutoCleaner(ConfigManager configManager) {
1    super(COMMON_CONFIG.getTTLCheckInterval());
1    this.configManager = configManager;
1    LOGGER.info(
1        "[PartitionTableCleaner] The PartitionTableAutoCleaner is started with cycle={}ms",
1        COMMON_CONFIG.getTTLCheckInterval());
1  }
1
1  @Override
1  protected void periodicExecute(Env env) {
1    List<String> databases = configManager.getClusterSchemaManager().getDatabaseNames(null);
1    Map<String, Long> databaseTTLMap = new TreeMap<>();
1    for (String database : databases) {
1      long databaseTTL =
1          PathUtils.isTableModelDatabase(database)
1              ? configManager.getClusterSchemaManager().getDatabaseMaxTTL(database)
1              : configManager.getTTLManager().getDatabaseMaxTTL(database);
1      databaseTTLMap.put(database, databaseTTL);
1    }
1    LOGGER.info(
1        "[PartitionTableCleaner] Periodically activate PartitionTableAutoCleaner, databaseTTL: {}",
1        databaseTTLMap);
1    for (String database : databases) {
1      long databaseTTL = databaseTTLMap.get(database);
1      if (!configManager.getPartitionManager().isDatabaseExist(database)
1          || databaseTTL < 0
1          || databaseTTL == Long.MAX_VALUE) {
1        // Remove the entry if the database or the TTL does not exist
1        databaseTTLMap.remove(database);
1      }
1    }
1    if (!databaseTTLMap.isEmpty()) {
1      LOGGER.info(
1          "[PartitionTableCleaner] Periodically activate PartitionTableAutoCleaner for: {}",
1          databaseTTLMap);
1      // Only clean the partition table when necessary
1      TTimePartitionSlot currentTimePartitionSlot = getCurrentTimePartitionSlot();
1      try {
1        configManager
1            .getConsensusManager()
1            .write(new AutoCleanPartitionTablePlan(databaseTTLMap, currentTimePartitionSlot));
1      } catch (ConsensusException e) {
1        LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
1      }
1    }
1  }
1
1  /**
1   * @return The time partition slot corresponding to current timestamp. Note that we do not shift
1   *     the start time to the correct starting point, since this interface only constructs a time
1   *     reference position for the partition table cleaner.
1   */
1  private static TTimePartitionSlot getCurrentTimePartitionSlot() {
1    if ("ms".equals(timestampPrecision)) {
1      return new TTimePartitionSlot(System.currentTimeMillis());
1    } else if ("us".equals(timestampPrecision)) {
1      return new TTimePartitionSlot(System.currentTimeMillis() * 1000);
1    } else {
1      return new TTimePartitionSlot(System.currentTimeMillis() * 1000_000);
1    }
1  }
1}
1