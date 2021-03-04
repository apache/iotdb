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

import org.apache.iotdb.db.conf.IoTDBConstant;

public class MetadataConstant {

  private MetadataConstant() {
    // allowed to do nothing
  }

  private static final String MTREE_VERSION = "1";

  public static final String ROOT = "root";
  public static final String METADATA_TXT_LOG = "mlog.txt";
  public static final String METADATA_LOG = "mlog.bin";
  public static final String TAG_LOG = "tlog.txt";
  public static final String MTREE_PREFIX = "mtree";
  public static final String MTREE_TXT_SNAPSHOT =
      MTREE_PREFIX + IoTDBConstant.FILE_NAME_SEPARATOR + MTREE_VERSION + ".snapshot";
  public static final String MTREE_SNAPSHOT =
      MTREE_PREFIX + IoTDBConstant.FILE_NAME_SEPARATOR + MTREE_VERSION + ".snapshot.bin";
  public static final String MTREE_SNAPSHOT_TMP =
      MTREE_PREFIX + IoTDBConstant.FILE_NAME_SEPARATOR + MTREE_VERSION + ".snapshot.bin.tmp";

  public static final short MNODE_TYPE = 0;
  public static final short STORAGE_GROUP_MNODE_TYPE = 1;
  public static final short MEASUREMENT_MNODE_TYPE = 2;
}
