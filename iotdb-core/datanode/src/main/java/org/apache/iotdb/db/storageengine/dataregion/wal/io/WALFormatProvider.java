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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/** Optional WAL/checkpoint format implementation, without prescribing algorithms or key storage. */
public interface WALFormatProvider {
  /**
   * Stable magic in the reserved {@code V<digits>-WAL} namespace, independent of the write policy.
   * It must not reuse a built-in version, so missing providers can be rejected without guessing V1.
   */
  byte[] getMagic();

  boolean isEnabled();

  /** Initializes a new file, including its magic and header; does not take channel ownership. */
  WALFileContext create(File file, FileChannel channel) throws IOException;

  /** Opens an existing extension-format file, leaving the channel at its first segment. */
  WALFileContext open(File file, FileChannel channel) throws IOException;

  WALMetaData readMetadata(File file, FileChannel channel) throws IOException;

  /** Enforces product policy before opening either a legacy or extension-format file. */
  default void validateRead(File file, WALFileVersion version) throws IOException {}

  /** Checks the file before replay or cleanup may have destructive side effects. */
  void validateForRecovery(File file) throws IOException;

  /** Product-specific diagnostics, called once when recovery terminates with a fatal failure. */
  default void onRecoveryFailure(Throwable failure) {}
}
