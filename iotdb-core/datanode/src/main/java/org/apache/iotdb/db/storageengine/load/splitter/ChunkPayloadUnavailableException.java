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

package org.apache.iotdb.db.storageengine.load.splitter;

/**
 * Thrown when the payload a {@link ChunkPayloadRef} points at cannot be read back, for example
 * because the staged file has already been cleaned up after COMMIT or ABORT.
 *
 * <p>Callers that only forward a piece (such as the WAL dispatcher) can fall back to serializing
 * the piece with its references instead of aborting the forward.
 */
public class ChunkPayloadUnavailableException extends RuntimeException {

  public ChunkPayloadUnavailableException(final String message) {
    super(message);
  }

  public ChunkPayloadUnavailableException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
