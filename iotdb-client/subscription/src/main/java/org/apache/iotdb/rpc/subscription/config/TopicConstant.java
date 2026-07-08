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

package org.apache.iotdb.rpc.subscription.config;

import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;

public class TopicConstant {

  public static final String PATH_KEY = "path";
  public static final String PATH_DEFAULT_VALUE = "root.**";
  public static final String PATTERN_KEY = "pattern";
  public static final String PATTERN_DEFAULT_VALUE = "root";

  public static final String DATABASE_KEY = "database";
  public static final String TABLE_KEY = "table";
  public static final String COLUMN_KEY = "column";
  public static final String RETENTION_BYTES_KEY = "retention.bytes";
  public static final String RETENTION_MS_KEY = "retention.ms";
  public static final String DATABASE_DEFAULT_VALUE = ".*";
  public static final String TABLE_DEFAULT_VALUE = ".*";
  public static final String COLUMN_DEFAULT_VALUE = ".*";

  public static final String START_TIME_KEY = "start-time";
  public static final String END_TIME_KEY = "end-time";
  public static final String NOW_TIME_VALUE = "now";

  public static final String MODE_KEY = "mode";
  public static final String MODE_LIVE_VALUE = "live";
  public static final String MODE_SNAPSHOT_VALUE = "snapshot";
  public static final String MODE_CONSENSUS_VALUE = "consensus";
  public static final String MODE_DEFAULT_VALUE = MODE_LIVE_VALUE;

  public static final String ORDER_MODE_KEY = "order-mode";
  public static final String ORDER_MODE_LEADER_ONLY_VALUE = "leader-only";
  public static final String ORDER_MODE_MULTI_WRITER_VALUE = "multi-writer";
  public static final String ORDER_MODE_PER_WRITER_VALUE = "per-writer";
  public static final String ORDER_MODE_DEFAULT_VALUE = ORDER_MODE_LEADER_ONLY_VALUE;

  public static final String FORMAT_KEY = "format";
  public static final String FORMAT_RECORD_HANDLER_VALUE = "SubscriptionRecordHandler";
  public static final String FORMAT_TS_FILE_VALUE = "SubscriptionTsFileHandler";
  public static final String FORMAT_DEFAULT_VALUE = FORMAT_RECORD_HANDLER_VALUE;

  @Deprecated
  public static final String FORMAT_SESSION_DATA_SETS_HANDLER_VALUE = FORMAT_RECORD_HANDLER_VALUE;

  @Deprecated public static final String FORMAT_TS_FILE_HANDLER_VALUE = FORMAT_TS_FILE_VALUE;

  public static final String LOOSE_RANGE_KEY = "loose-range";
  public static final String LOOSE_RANGE_TIME_VALUE = "time";
  public static final String LOOSE_RANGE_PATH_VALUE = "path";
  public static final String LOOSE_RANGE_ALL_VALUE = "all";
  public static final String LOOSE_RANGE_DEFAULT_VALUE = "";

  public static final String STRICT_KEY = "strict";
  public static final String STRICT_DEFAULT_VALUE = "true";

  public static final String OWNER_ID_KEY = "owner-id";
  public static final String OWNER_EPOCH_KEY = "owner-epoch";
  // Highest owner epoch ever granted for this topic. Retained across clearOwner so that owner
  // epoch is globally monotonic and never reused, even after a clear -> re-enable cycle. Stored as
  // a topic attribute so it survives serialization automatically (self-describing).
  public static final String MAX_OWNER_EPOCH_KEY = "max-owner-epoch";
  public static final String OWNER_LEASE_DURATION_MS_KEY = "owner-lease-duration-ms";

  private TopicConstant() {
    throw new IllegalStateException(SubscriptionMessages.UTILITY_CLASS);
  }
}
