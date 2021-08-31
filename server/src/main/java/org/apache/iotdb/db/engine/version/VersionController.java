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

package org.apache.iotdb.db.engine.version;

/**
 * VersionController manages the version(a monotonically increasing long) of a storage group. We
 * define that each memtable flush, data deletion, or data update will generate a new version of
 * dataset. So upon the above actions are performed, a new version number is generated and assigned
 * to such actions. Additionally, we also assign versions to TsFiles in their file names, so
 * hopefully we will compare files directly across IoTDB replicas. NOTICE: Thread-safety should be
 * guaranteed by the caller.
 */
public interface VersionController {
  /**
   * Get the next version number.
   *
   * @return the next version number.
   */
  long nextVersion();

  /**
   * Get the current version number.
   *
   * @return the current version number.
   */
  long currVersion();
}
