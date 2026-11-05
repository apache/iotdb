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

package org.apache.iotdb.db.i18n;

public final class DataNodeSchemaMessages {

  private DataNodeSchemaMessages() {}

  public static final String
      EXCEPTION_CANNOT_RECOVER_DUPLICATED_SCHEMAREGION_ARG_FOUND_IN_DATABASES_ARG_AND_ARG_D9F60E05 =
          "Cannot recover duplicated SchemaRegion [%s] found in databases [%s] and [%s].";

  public static final String FAILED_TO_RECOVER_TAG_INDEX =
      "Failed to recover tag index for measurement [{}] in database [{}], SchemaRegion [{}], tag offset [{}].";
  public static final String SCHEMA_REGION_FAILED_TO_RECOVER =
      "SchemaRegion [%d] in StorageGroup [%s] failed to recover.";
  public static final String SDT_COMPRESSION_DEVIATION_REQUIRED =
      "SDT compression requires the compdev property";
  public static final String SDT_COMPRESSION_DEVIATION_NEGATIVE =
      "SDT property compdev must be non-negative, but was [%s]";
  public static final String SDT_COMPRESSION_DEVIATION_FORMAT_ERROR =
      "SDT property compdev must be a number, but was [%s]";
  public static final String SDT_COMPRESSION_MAX_GREATER_THAN_MIN =
      "SDT property compmaxtime [%s] must be greater than compmintime [%s]";
  public static final String SDT_COMPRESSION_TIME_NEGATIVE =
      "SDT property %s must be non-negative, but was [%s]";
  public static final String SDT_COMPRESSION_TIME_FORMAT_ERROR =
      "SDT property %s must be a 64-bit integer, but was [%s]";
  public static final String SDT_ENABLED_NO_COMPRESSION_TIME =
      "Timeseries [{}] uses SDT without property [{}]; using default [{}].";
  public static final String CREATE_TEMPLATE_ERROR =
      "Failed to create schema template [%s] on ConfigNode: %s";
  public static final String GET_ALL_TEMPLATE_ERROR =
      "Failed to get schema templates from ConfigNode: %s";
  public static final String GET_TEMPLATE_INFO_ERROR =
      "Failed to get schema template [%s] from ConfigNode: %s";
  public static final String ILLEGAL_PATH_LOG = "illegal path {}";
}
