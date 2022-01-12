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
 *
 */
package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.exception.PipeException;

public interface Pipe {
  void start();

  void stop();

  void drop();

  String getName();

  PipeSink getPipeSink();

  long getCreateTime();

  PipeStatus getStatus();

  String serialize();

  enum PipeStatus {
    RUNNING,
    STOP,
    DROP
  }

  class PipeFactory {
    // when adding a new type pipe, should write a factory method to build it from bytebuffer
    public static Pipe createPipe(String className, String serializationString)
        throws PipeException {
      if (TsFilePipe.class.getName().equals(className)) {}
      throw new UnsupportedOperationException("Not support for pipe type " + className);
    }
  }
}
