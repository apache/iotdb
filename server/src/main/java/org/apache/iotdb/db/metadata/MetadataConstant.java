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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;

public class MetadataConstant {

  private MetadataConstant() {
    // allowed to do nothing
  }

  public static final String MTREE_PREFIX = "mtree";
  private static final String MTREE_VERSION = "1";
  public static final String MTREE_TXT_SNAPSHOT_OLD_VERSION =
      MTREE_PREFIX + IoTDBConstant.FILE_NAME_SEPARATOR + MTREE_VERSION + ".snapshot";
  public static final String MTREE_SNAPSHOT_OLD_VERSION =
      MTREE_PREFIX + IoTDBConstant.FILE_NAME_SEPARATOR + MTREE_VERSION + ".snapshot.bin";
  public static final String MTREE_SNAPSHOT_TMP_OLDVERSION =
      MTREE_PREFIX + IoTDBConstant.FILE_NAME_SEPARATOR + MTREE_VERSION + ".snapshot.bin.tmp";

  public static final String ROOT = "root";
  public static final String METADATA_TXT_LOG = "mlog.txt";
  public static final String METADATA_LOG = "mlog.bin";
  public static final String METADATA_LOG_DESCRIPTION = "mlog.description";
  public static final String TAG_LOG = "tlog.txt";
  public static final String TEMPLATE_FILE = "template_log.bin";
  public static final String STORAGE_GROUP_LOG = "storage_group_log.bin";
  public static final String SCHEMA_FILE_NAME = "schema_file.pst";
  public static final String SCHEMA_LOG_FILE_NAME = "schema_file_log.bin";

  public static final String SCHEMA_FILE_SNAPSHOT = "schema_file.pst.snapshot";
  public static final String TAG_LOG_SNAPSHOT = "tlog.txt.snapshot";
  public static final String TAG_LOG_SNAPSHOT_TMP = "tlog.txt.snapshot.tmp";
  public static final String MTREE_SNAPSHOT = "mtree.snapshot";
  public static final String MTREE_SNAPSHOT_TMP = "mtree.snapshot.tmp";

  public static final String[] ALL_RESULT_NODES = new String[] {"root", "**"};
  public static final PartialPath ALL_MATCH_PATTERN = new PartialPath(new String[] {"root", "**"});

  public static final int NON_TEMPLATE = -1;
  public static final int ALL_TEMPLATE = -2;

  public static final byte INTERNAL_MNODE_TYPE = 0;
  public static final byte STORAGE_GROUP_MNODE_TYPE = 1;
  public static final byte MEASUREMENT_MNODE_TYPE = 2;
  public static final byte ENTITY_MNODE_TYPE = 3;
  public static final byte STORAGE_GROUP_ENTITY_MNODE_TYPE = 4;

  public static final String INTERNAL_MNODE_TYPE_NAME = "InternalMNode";
  public static final String STORAGE_GROUP_MNODE_TYPE_NAME = "StorageGroupMNode";
  public static final String MEASUREMENT_MNODE_TYPE_NAME = "MeasurementMNode";
  public static final String ENTITY_MNODE_TYPE_NAME = "EntityMNode";
  public static final String STORAGE_GROUP_ENTITY_MNODE_TYPE_NAME = "StorageGroupEntityMNode";

  public static final String SCHEMA_REGION_METRIC_NAME = "schema_region";
  public static final String SCHEMA_ENGINE_METRIC_NAME = "schema_file";

  public static final String DEFAULT_SCHEMA_ENGINE_MODE = "Memory";

  public static String getMNodeTypeName(byte type) {
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        return INTERNAL_MNODE_TYPE_NAME;
      case STORAGE_GROUP_MNODE_TYPE:
        return STORAGE_GROUP_MNODE_TYPE_NAME;
      case MEASUREMENT_MNODE_TYPE:
        return MEASUREMENT_MNODE_TYPE_NAME;
      case ENTITY_MNODE_TYPE:
        return ENTITY_MNODE_TYPE_NAME;
      case STORAGE_GROUP_ENTITY_MNODE_TYPE:
        return STORAGE_GROUP_ENTITY_MNODE_TYPE_NAME;
      default:
        throw new RuntimeException("Undefined MNode type " + type);
    }
  }

  public static boolean isStorageGroupType(byte type) {
    return type == STORAGE_GROUP_MNODE_TYPE || type == STORAGE_GROUP_ENTITY_MNODE_TYPE;
  }
}
