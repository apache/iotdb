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

package org.apache.iotdb.db.pipe.sink.util;

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
  public static List<Pair<String, TEndPoint>> parseRecommendedRedirections(final TSStatus status) {
    // Each top-level sub-status corresponds to one statement constructed by the receiver. Batch
    // requests may contain any number of statements, and a direct InsertRowsNode request may put
    // the per-device redirect statuses directly at the top level.
    final List<Pair<String, TEndPoint>> redirectList = new ArrayList<>();

    if (status == null || status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      return redirectList;
    }

    if (status.isSetSubStatus()) {
      for (final TSStatus subStatus : status.getSubStatus()) {
        if (subStatus != null) {
          collectRedirects(subStatus, redirectList);
        }
      }
    }

    return redirectList;
  }

  private static void collectRedirects(
      final TSStatus status, final List<Pair<String, TEndPoint>> redirectList) {
    addRedirectIfPresent(redirectList, status);
    if (status.isSetSubStatus()) {
      for (final TSStatus subStatus : status.getSubStatus()) {
        if (subStatus != null) {
          collectRedirects(subStatus, redirectList);
        }
      }
    }
  }

  private static void addRedirectIfPresent(
      final List<Pair<String, TEndPoint>> redirectList, final TSStatus status) {
    if (status.isSetRedirectNode() && status.isSetMessage() && !status.getMessage().isEmpty()) {
      // The receiver sets the message to a device path only when it can safely associate the
      // redirection with a single tree-model device.
      redirectList.add(new Pair<>(status.getMessage(), status.getRedirectNode()));
    }
  }
}
