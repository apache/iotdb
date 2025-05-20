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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.consensus.request.write.partition.AutoCleanPartitionTablePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.consensus.exception.ConsensusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.iotdb.confignode.manager.partition.PartitionManager.CONSENSUS_WRITE_ERROR;

/** A cleaner that automatically deletes the expired mapping within the partition table. */
public class PartitionTableAutoCleaner<Env> extends InternalProcedure<Env> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionTableAutoCleaner.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final String timestampPrecision =
      CommonDescriptor.getInstance().getConfig().getTimestampPrecision();

  private final ConfigManager configManager;

  public PartitionTableAutoCleaner(ConfigManager configManager) {
    super(COMMON_CONFIG.getTTLCheckInterval());
    this.configManager = configManager;
    LOGGER.info(
        "[PartitionTableCleaner] The PartitionTableAutoCleaner is started with cycle={}ms",
        COMMON_CONFIG.getTTLCheckInterval());
  }

  @Override
  protected void periodicExecute(Env env) {
    List<String> databases = configManager.getClusterSchemaManager().getDatabaseNames(null);
    Map<String, Long> databaseTTLMap = new TreeMap<>();
    for (String database : databases) {
      long databaseTTL =
          PathUtils.isTableModelDatabase(database)
              ? configManager.getClusterSchemaManager().getDatabaseMaxTTL(database)
              : configManager.getTTLManager().getDatabaseMaxTTL(database);
      databaseTTLMap.put(database, databaseTTL);
    }
    LOGGER.info(
        "[PartitionTableCleaner] Periodically activate PartitionTableAutoCleaner, databaseTTL: {}",
        databaseTTLMap);
    for (String database : databases) {
      long databaseTTL = databaseTTLMap.get(database);
      if (!configManager.getPartitionManager().isDatabaseExist(database)
          || databaseTTL < 0
          || databaseTTL == Long.MAX_VALUE) {
        // Remove the entry if the database or the TTL does not exist
        databaseTTLMap.remove(database);
      }
    }
    if (!databaseTTLMap.isEmpty()) {
      LOGGER.info(
          "[PartitionTableCleaner] Periodically activate PartitionTableAutoCleaner for: {}",
          databaseTTLMap);
      // Only clean the partition table when necessary
      TTimePartitionSlot currentTimePartitionSlot = getCurrentTimePartitionSlot();
      try {
        configManager
            .getConsensusManager()
            .write(new AutoCleanPartitionTablePlan(databaseTTLMap, currentTimePartitionSlot));
      } catch (ConsensusException e) {
        LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      }
    }
  }

  /**
   * @return The time partition slot corresponding to current timestamp. Note that we do not shift
   *     the start time to the correct starting point, since this interface only constructs a time
   *     reference position for the partition table cleaner.
   */
  private static TTimePartitionSlot getCurrentTimePartitionSlot() {
    if ("ms".equals(timestampPrecision)) {
      return new TTimePartitionSlot(System.currentTimeMillis());
    } else if ("us".equals(timestampPrecision)) {
      return new TTimePartitionSlot(System.currentTimeMillis() * 1000);
    } else {
      return new TTimePartitionSlot(System.currentTimeMillis() * 1000_000);
    }
  }
}
