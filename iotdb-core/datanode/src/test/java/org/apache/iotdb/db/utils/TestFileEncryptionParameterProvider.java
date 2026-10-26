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

import org.apache.tsfile.encrypt.ChunkCryptoContext;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptionProviderRegistry;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.encrypt.IEncrypt;
import org.apache.tsfile.encrypt.IEncryptProvider;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.encrypt.PageCryptoContext;
import org.apache.tsfile.exception.encrypt.EncryptException;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test-only framing and context checks. This is deliberately not a cryptographic implementation.
 */
public class TestFileEncryptionParameterProvider implements FileEncryptionParameterProvider {
  public static final String ID = "iotdb-test-file-context";
  private static final ThreadLocal<Boolean> ENABLED = ThreadLocal.withInitial(() -> false);
  public static final ThreadLocal<Boolean> RETURN_NULL = ThreadLocal.withInitial(() -> false);
  private static final AtomicLong IDS = new AtomicLong();
  public static final AtomicInteger CREATED = new AtomicInteger();
  public static final AtomicInteger CLOSED = new AtomicInteger();

  public static void enable() {
    ENABLED.set(true);
    CREATED.set(0);
    CLOSED.set(0);
    EncryptionProviderRegistry.registerProvider(new FramingProvider());
  }

  public static void disable() {
    ENABLED.remove();
    RETURN_NULL.remove();
    EncryptionProviderRegistry.unregisterProvider(ID);
  }

  @Override
  public boolean requiresFileRewrite() {
    return ENABLED.get();
  }

  @Override
  public EncryptParameter createForNewFile(File file, String database, EncryptParameter legacy) {
    if (RETURN_NULL.get()) {
      return null;
    }
    return ENABLED.get() ? newParameter() : legacy;
  }

  public static EncryptParameter newParameter() {
    return EncryptParameter.pageAeadBuilder()
        .providerId(ID)
        .profileId("TEST_FRAMING")
        .keyId("test")
        .keyVersion("1")
        .wrappedDataKey(new byte[] {1})
        .fileCryptoId(ByteBuffer.allocate(16).putLong(IDS.incrementAndGet()).array())
        .build();
  }

  private static byte[] digest(byte[] context, byte[] data) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update(context);
    return digest.digest(data);
  }

  public static class FramingProvider implements IEncryptProvider {
    @Override
    public String getProviderId() {
      return ID;
    }

    @Override
    public IEncrypt create(EncryptParameter parameter) {
      CREATED.incrementAndGet();
      IEncryptor encryptor = mock(IEncryptor.class);
      IDecryptor decryptor = mock(IDecryptor.class);
      when(encryptor.encryptPage(any(), anyInt(), anyInt(), any()))
          .thenAnswer(
              call -> {
                byte[] data = call.getArgument(0);
                int offset = call.getArgument(1);
                int size = call.getArgument(2);
                PageCryptoContext context = call.getArgument(3);
                byte[] plaintext = Arrays.copyOfRange(data, offset, offset + size);
                return ByteBuffer.allocate(size + 32)
                    .put(plaintext)
                    .put(digest(context.getAssociatedData(), plaintext))
                    .array();
              });
      when(decryptor.decryptPage(any(), anyInt(), anyInt(), any()))
          .thenAnswer(
              call -> {
                byte[] data = call.getArgument(0);
                int offset = call.getArgument(1);
                int size = call.getArgument(2);
                PageCryptoContext context = call.getArgument(3);
                byte[] plaintext = Arrays.copyOfRange(data, offset, offset + size - 32);
                if (!Arrays.equals(
                    digest(context.getAssociatedData(), plaintext),
                    Arrays.copyOfRange(data, offset + size - 32, offset + size))) {
                  throw new EncryptException("test page context mismatch");
                }
                return plaintext;
              });
      return new IEncrypt() {
        @Override
        public IEncryptor getEncryptor() {
          return encryptor;
        }

        @Override
        public IDecryptor getDecryptor() {
          return decryptor;
        }

        @Override
        public int getPageBodyOverhead() {
          return 32;
        }

        @Override
        public int getChunkAuthenticationSize() {
          return 32;
        }

        @Override
        public byte[] authenticateChunk(ChunkCryptoContext context) {
          try {
            return digest(context.getAssociatedData(), new byte[0]);
          } catch (Exception e) {
            throw new AssertionError(e);
          }
        }

        @Override
        public void verifyChunkAuthentication(byte[] proof, ChunkCryptoContext context) {
          if (!Arrays.equals(proof, authenticateChunk(context))) {
            throw new EncryptException("test chunk context mismatch");
          }
        }

        @Override
        public void close() {
          CLOSED.incrementAndGet();
        }
      };
    }
  }
}
