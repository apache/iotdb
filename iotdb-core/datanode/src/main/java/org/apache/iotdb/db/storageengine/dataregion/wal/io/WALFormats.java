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

import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ServiceLoader;

/** Dispatches optional formats without exposing their binary layouts to the standard WAL path. */
public final class WALFormats {
  private WALFormats() {}

  private static class Holder {
    private static final WALFormatProvider PROVIDER =
        selectProvider(ServiceLoader.load(WALFormatProvider.class).iterator());
  }

  static WALFormatProvider selectProvider(Iterator<WALFormatProvider> providers) {
    if (!providers.hasNext()) {
      return null;
    }
    WALFormatProvider provider = providers.next();
    if (providers.hasNext()) {
      throw new IllegalStateException(
          StorageEngineMessages.EXCEPTION_MULTIPLE_WAL_FORMAT_PROVIDERS_ARE_CONFIGURED_DD78877D);
    }
    byte[] magic = provider.getMagic();
    if (magic == null
        || magic.length > 64
        || !new String(magic, StandardCharsets.US_ASCII).matches("V[0-9]+-WAL")
        || Arrays.equals(magic, WALFileVersion.V2.getVersionBytes())
        || Arrays.equals(magic, WALFileVersion.V3.getVersionBytes())) {
      throw new IllegalStateException(
          StorageEngineMessages.EXCEPTION_UNSUPPORTED_OR_UNAVAILABLE_WAL_FORMAT_4629CFE4);
    }
    return provider;
  }

  public static WALFileVersion writeVersion(WALFileVersion requested) {
    return Holder.PROVIDER != null && Holder.PROVIDER.isEnabled()
        ? WALFileVersion.EXTENDED
        : requested;
  }

  static boolean recognizes(FileChannel channel) throws IOException {
    return recognizes(channel, Holder.PROVIDER);
  }

  static boolean recognizes(FileChannel channel, WALFormatProvider provider) throws IOException {
    long position = channel.position();
    try {
      if (provider != null) {
        byte[] magic = provider.getMagic();
        if (channel.size() >= magic.length) {
          ByteBuffer bytes = ByteBuffer.allocate(magic.length);
          IOUtils.readFully(channel, bytes, 0);
          if (Arrays.equals(magic, bytes.array())) {
            return true;
          }
        }
      }
      // Versioned magic is reserved: an unavailable future format must not fall back to V1.
      int size = (int) Math.min(channel.size(), 64);
      ByteBuffer prefix = ByteBuffer.allocate(size);
      IOUtils.readFully(channel, prefix, 0);
      if (new String(prefix.array(), StandardCharsets.US_ASCII).matches("(?s)^V[0-9]+-WAL.*")) {
        throw WALFormatException.unsupported();
      }
      return false;
    } finally {
      channel.position(position);
    }
  }

  public static WALFileContext create(File file, FileChannel channel) throws IOException {
    if (Holder.PROVIDER == null || !Holder.PROVIDER.isEnabled()) {
      throw WALFormatException.unsupported();
    }
    return requireContext(Holder.PROVIDER.create(file, channel));
  }

  public static WALFileContext open(File file, FileChannel channel) throws IOException {
    if (Holder.PROVIDER == null) {
      throw WALFormatException.unsupported();
    }
    return requireContext(Holder.PROVIDER.open(file, channel));
  }

  private static WALFileContext requireContext(WALFileContext context) throws WALFormatException {
    if (context == null) {
      throw WALFormatException.unsupported();
    }
    return context;
  }

  public static void validateRead(File file, WALFileVersion version) throws IOException {
    if (Holder.PROVIDER != null) {
      Holder.PROVIDER.validateRead(file, version);
    }
  }

  public static WALMetaData readMetadata(File file, FileChannel channel) throws IOException {
    if (Holder.PROVIDER == null) {
      throw WALFormatException.unsupported();
    }
    Holder.PROVIDER.validateRead(file, WALFileVersion.EXTENDED);
    return Holder.PROVIDER.readMetadata(file, channel);
  }

  public static void validateForRecovery(File file) throws IOException {
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      WALFileVersion version = WALFileVersion.getVersion(channel);
      validateRead(file, version);
      if (version == WALFileVersion.EXTENDED) {
        Holder.PROVIDER.validateForRecovery(file);
      }
    }
  }

  public static void onRecoveryFailure(Throwable failure) {
    if (Holder.PROVIDER != null) {
      Holder.PROVIDER.onRecoveryFailure(failure);
    }
  }
}
