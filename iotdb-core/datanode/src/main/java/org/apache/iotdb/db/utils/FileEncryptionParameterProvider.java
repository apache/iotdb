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
package org.apache.iotdb.db.utils;

import org.apache.tsfile.encrypt.EncryptParameter;

import java.io.File;

/**
 * Optional file-level policy, discovered once through {@link java.util.ServiceLoader}.
 *
 * <p>File-scoped compaction contexts are supported only by ReadPoint compaction. An extension using
 * them must select ReadPoint; this SPI does not adapt or redirect Fast/ReadChunk compaction.
 */
@FunctionalInterface
public interface FileEncryptionParameterProvider {
  /**
   * Returns non-null parameters for exactly one new file. The legacy parameter is borrowed and must
   * not be modified or closed. A returned page-AEAD descriptor is owned by the caller; generation
   * and key resolution belong to the provider, not to the storage engine.
   */
  EncryptParameter createForNewFile(File file, String database, EncryptParameter legacyParameter);

  /** Whether Load must rewrite input files through the regular tablet write pipeline. */
  default boolean requiresFileRewrite() {
    return false;
  }
}
