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

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public interface UpdateContainer {

  long updateAttribute(
      final String tableName, final String[] deviceId, final Map<String, Binary> updatedAttributes);

  // Only this method is not synchronize called and is called by GRASS thread
  // A piece of "updateContent" won't exceed "limitBytes" in order to handle
  // thrift threshold and low bandwidth
  // Note that a "piece" returned is also a complete "updateContainer"'s serialized bytes
  // The "limitBytes" shall be at least 5 for a "type" and "0" to indicate empty
  byte[] getUpdateContent(
      final @Nonnull AtomicInteger limitBytes, final @Nonnull AtomicBoolean hasRemaining);

  long invalidate(final String tableName);

  long invalidate(final String[] pathNodes);

  long invalidate(final String tableName, final String attributeName);

  Pair<Long, Boolean> updateSelfByCommitContainer(final UpdateContainer commitContainer);

  void serialize(final OutputStream outputstream) throws IOException;

  void deserialize(final InputStream inputStream) throws IOException;
}
