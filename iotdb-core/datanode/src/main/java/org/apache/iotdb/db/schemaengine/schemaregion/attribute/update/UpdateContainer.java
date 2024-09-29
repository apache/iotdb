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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.tsfile.utils.Pair;

import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

@ThreadSafe
public interface UpdateContainer {

  long updateAttribute(
      final String tableName, final String[] deviceId, final Map<String, String> updatedAttributes);

  // Only this method is not synchronize called and is called by GRASS thread
  // A piece of "updateContent" won't exceed "limitBytes" in order to handle
  // thrift threshold and low bandwidth
  default byte[] getUpdateContent(final int limitBytes) {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      serialize(outputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return outputStream.toByteArray();
  }

  Pair<Integer, Boolean> updateSelfByCommitBuffer(final ByteBuffer commitBuffer);

  void serialize(final OutputStream outputstream) throws IOException;

  void deserialize(final InputStream inputStream) throws IOException;
}
