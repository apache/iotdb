/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.tsfile.utils.PublicBAOS;

import java.nio.ByteBuffer;

public final class LoadTsFilePieceNodeAssembler {

  private final int sliceCount;
  private final int originBodySize;
  private final PublicBAOS assembledBody = new PublicBAOS();

  private int nextSliceIndex;

  LoadTsFilePieceNodeAssembler(final int sliceCount, final int originBodySize) {
    this.sliceCount = sliceCount;
    this.originBodySize = originBodySize;
  }

  synchronized Result append(
      final ByteBuffer sliceBody,
      final int sliceIndex,
      final int requestSliceCount,
      final int requestOriginBodySize) {
    if (sliceBody == null
        || !sliceBody.hasRemaining()
        || sliceCount <= 1
        || originBodySize <= 0
        || sliceCount != requestSliceCount
        || originBodySize != requestOriginBodySize
        || sliceIndex != nextSliceIndex
        || sliceIndex < 0
        || sliceIndex >= sliceCount
        || assembledBody.size() > originBodySize - sliceBody.remaining()) {
      return Result.invalid();
    }

    final ByteBuffer duplicatedBody = sliceBody.duplicate();
    if (duplicatedBody.hasArray()) {
      assembledBody.write(
          duplicatedBody.array(),
          duplicatedBody.arrayOffset() + duplicatedBody.position(),
          duplicatedBody.remaining());
    } else {
      final byte[] bytes = new byte[Math.min(duplicatedBody.remaining(), 8192)];
      while (duplicatedBody.hasRemaining()) {
        final int size = Math.min(duplicatedBody.remaining(), bytes.length);
        duplicatedBody.get(bytes, 0, size);
        assembledBody.write(bytes, 0, size);
      }
    }
    nextSliceIndex++;

    if (nextSliceIndex < sliceCount) {
      return assembledBody.size() < originBodySize ? Result.incomplete() : Result.invalid();
    }
    if (assembledBody.size() != originBodySize) {
      return Result.invalid();
    }
    return Result.complete(
        ByteBuffer.wrap(assembledBody.getBuf(), 0, assembledBody.size()).asReadOnlyBuffer());
  }

  public static final class Result {

    private static final Result INCOMPLETE = new Result(true, null);
    private static final Result INVALID = new Result(false, null);

    private final boolean valid;
    private final ByteBuffer body;

    private Result(final boolean valid, final ByteBuffer body) {
      this.valid = valid;
      this.body = body;
    }

    static Result incomplete() {
      return INCOMPLETE;
    }

    static Result invalid() {
      return INVALID;
    }

    static Result complete(final ByteBuffer body) {
      return new Result(true, body);
    }

    public boolean isValid() {
      return valid;
    }

    public boolean isComplete() {
      return body != null;
    }

    public ByteBuffer getBody() {
      return body;
    }
  }
}
