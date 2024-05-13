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

package org.apache.iotdb.db.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LeaderCacheUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCacheUtils.class);

  private LeaderCacheUtils() {
    // Do nothing
  }

  /**
   * Get all redirection recommends after transferring a batch event to update leader cache.
   *
   * @param status is the returned status after transferring a batch event.
   * @return a list of pairs, each pair contains a device path and its redirect endpoint.
   */
  public static List<Pair<String, TEndPoint>> getRedirectionsAfterTransferBatch(TSStatus status) {
    // If there is no exception, there should be 2 sub-statuses, one for InsertRowsStatement and one
    // for InsertMultiTabletsStatement (see IoTDBDataNodeReceiver#handleTransferTabletBatch).
    List<Pair<String, TEndPoint>> redirectList = new ArrayList<>();
    if (status.getSubStatusSize() != 2) {
      return redirectList;
    }

    for (TSStatus subStatus : status.getSubStatus()) {
      if (subStatus.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        continue;
      }

      for (TSStatus innerSubStatus : subStatus.getSubStatus()) {
        if (innerSubStatus.isSetRedirectNode()) {
          try {
            new PartialPath(innerSubStatus.getMessage());
            // The message field should be a device path.
            redirectList.add(
                new Pair<>(innerSubStatus.getMessage(), innerSubStatus.getRedirectNode()));
          } catch (IllegalPathException e) {
            LOGGER.warn(
                "Illegal device path when updating leader cache: {}",
                innerSubStatus.getMessage(),
                e);
          }
        }
      }
    }
    return redirectList;
  }
}
