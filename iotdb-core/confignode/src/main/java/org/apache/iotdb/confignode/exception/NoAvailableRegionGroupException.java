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

package org.apache.iotdb.confignode.exception;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;

import java.util.List;

public class NoAvailableRegionGroupException extends ConfigNodeException {

  private static final String SCHEMA_REGION_GROUP =
      ConfigNodeMessages.EXCEPTION_SCHEMAREGIONGROUPS_3C409207;
  private static final String DATA_REGION_GROUP =
      ConfigNodeMessages.EXCEPTION_DATAREGIONGROUPS_945CCE78;

  public NoAvailableRegionGroupException(
      final TConsensusGroupType regionGroupType, final List<String> databases) {
    super(
        String.format(
            ConfigNodeMessages.EXCEPTION_THERE_NO_AVAILABLE_ARG_DATABASES_ARG_CURRENTLY_9B8297B3
                + ConfigNodeMessages
                    .EXCEPTION_PLEASE_USE_SHOW_CLUSTER_SHOW_REGIONS_CHECK_CLUSTER_STATUS_611120DF,
            TConsensusGroupType.SchemaRegion.equals(regionGroupType)
                ? SCHEMA_REGION_GROUP
                : DATA_REGION_GROUP,
            databases));
  }
}
