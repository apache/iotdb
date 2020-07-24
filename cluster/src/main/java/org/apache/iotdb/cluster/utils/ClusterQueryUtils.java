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

package org.apache.iotdb.cluster.utils;

import java.util.Collections;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.read.common.Path;

public class ClusterQueryUtils {

  private ClusterQueryUtils() {
    // util class
  }

  /**
   * Check if the given path exists locally or can be pulled from a remote node.
   * @param path
   * @param metaGroupMember
   * @throws QueryProcessException
   */
  public static void checkPathExistence(Path path, MetaGroupMember metaGroupMember)
      throws QueryProcessException {
    checkPathExistence(path.getFullPath(), metaGroupMember);
  }

  public static void checkPathExistence(String path, MetaGroupMember metaGroupMember)
      throws QueryProcessException {
    if (!IoTDB.metaManager.isPathExist(path)) {
      try {
        metaGroupMember.pullTimeSeriesSchemas(Collections.singletonList(path), null);
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }
  }
}
