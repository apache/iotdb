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

package org.apache.tsfile.file.metadata.enums;

public enum EncryptionType {
  /** UNENCRYPTED. */
  UNENCRYPTED("UNENCRYPTED", (byte) 0),

  /** SM4128. */
  SM4128("SM4128", (byte) 1),

  /** AES128. */
  AES128("AES128", (byte) 2);

  private final String extensionName;
  private final byte index;

  EncryptionType(String extensionName, byte index) {
    this.extensionName = extensionName;
    this.index = index;
  }

  /**
   * deserialize byte number.
   *
   * @param encryptor byte number
   * @return CompressionType
   * @throws IllegalArgumentException illegal argument
   */
  public static EncryptionType deserialize(byte encryptor) {
    switch (encryptor) {
      case 0:
        return EncryptionType.UNENCRYPTED;
      case 1:
        return EncryptionType.SM4128;
      case 2:
        return EncryptionType.AES128;
      default:
        throw new IllegalArgumentException("Invalid input: " + encryptor);
    }
  }

  public static int getSerializedSize() {
    return Byte.BYTES;
  }

  /**
   * get serialized size.
   *
   * @return byte of index
   */
  public byte serialize() {
    return this.index;
  }

  /**
   * get extension.
   *
   * @return extension (string type), for example: UNENCRYPTED、AES128
   */
  public String getExtension() {
    return extensionName;
  }
}
