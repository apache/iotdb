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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

/**
 * Memory block used by a tsfile parser.
 *
 * <p>The parser is shared by Pipe and Load. Keeping the block behind this small interface allows
 * the same parsing code to use the owning subsystem's memory pool instead of hard-coding the Pipe
 * pool.
 */
public interface TsFileInsertionEventParserMemoryBlock extends AutoCloseable {

  long getMemoryUsageInBytes();

  void forceResize(long newSizeInBytes);

  @Override
  void close();
}
