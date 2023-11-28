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

package org.apache.iotdb.commons.pipe.task.meta.compatibility.runtimemeta;

import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.utils.TestOnly;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * The interface for all outdated PipeRuntimeMetas. With this, we are able to intuitively test the
 * compatibility between the previous ones and the current one, ease the transformation of old
 * snapShot to incumbent class instance, and avoid the dilation of the deserialization method
 * numbers emerging in the working process.
 *
 * <p>The current version of {@link PipeRuntimeMeta} is PipeRuntimeMetaV2_2.
 */
interface FormerPipeRuntimeMeta {

  @TestOnly
  ByteBuffer serialize() throws IOException;

  @TestOnly
  void serialize(OutputStream outputStream) throws IOException;

  /**
   * This method always return the incumbent {@link PipeRuntimeMeta}. When switching to a new
   * version, be sure to move the previous one to the packet, grant it a version, make it implement
   * this interface, and then update this method in all {@link FormerPipeRuntimeMeta}s.
   */
  PipeRuntimeMeta toCurrentPipeRuntimeMetaVersion();
}
