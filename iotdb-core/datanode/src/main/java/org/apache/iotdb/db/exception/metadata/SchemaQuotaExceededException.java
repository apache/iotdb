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

package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.ClusterSchemaQuotaLevel;
import org.apache.iotdb.rpc.TSStatusCode;

public class SchemaQuotaExceededException extends MetadataException {

  // used for timeseries/device limit
  public SchemaQuotaExceededException(ClusterSchemaQuotaLevel level, long limit) {
    super(
        String.format(
            "The current metadata capacity has exceeded the cluster quota. "
                + "The cluster quota is set at the %s level, with a limit number of %d. "
                + "Please review your configuration "
                + "or delete some existing time series to comply with the quota.",
            level.toString(), limit),
        TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode());
  }

  // used for database limit
  public SchemaQuotaExceededException(long limit) {
    super(
        String.format(
            "The current database number has exceeded the cluster quota. "
                + "The maximum number of cluster databases allowed is %d, "
                + "Please review your configuration database_limit_threshold "
                + "or delete some existing database to comply with the quota.",
            limit),
        TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode());
  }
}
