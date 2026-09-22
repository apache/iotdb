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

package org.apache.iotdb.db.pipe.sink.util.cacher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class LeaderCacheUtils {

  private LeaderCacheUtils() {
    // Do nothing
  }

  /**
   * Get all redirection recommends after transferring a batch event to update leader cache.
   *
   * @param status is the returned status after transferring a batch event.
   * @return a list of pairs, each pair contains a device path and its redirect endpoint.
   */
  public static List<Pair<String, TEndPoint>> parseRecommendedRedirections(TSStatus status) {
    // Each top-level sub-status corresponds to one statement constructed by the receiver. V2 batch
    // requests may contain any number of statements because rows are grouped by database and table.
    final List<Pair<String, TEndPoint>> redirectList = new ArrayList<>();

    if (status == null || !status.isSetSubStatus()) {
      return redirectList;
    }

    for (final TSStatus subStatus : status.getSubStatus()) {
      if (subStatus == null
          || subStatus.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
          || !subStatus.isSetSubStatus()) {
        continue;
      }

      for (final TSStatus innerSubStatus : subStatus.getSubStatus()) {
        if (innerSubStatus != null
            && innerSubStatus.isSetRedirectNode()
            && innerSubStatus.isSetMessage()
            && !innerSubStatus.getMessage().isEmpty()) {
          // The receiver sets the message to a device path only when it can safely associate the
          // redirection with a single tree-model device.
          redirectList.add(
              new Pair<>(innerSubStatus.getMessage(), innerSubStatus.getRedirectNode()));
        }
      }
    }

    return redirectList;
  }
}
