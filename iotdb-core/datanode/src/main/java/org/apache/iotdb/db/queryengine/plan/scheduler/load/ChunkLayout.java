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
 */
package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Chunk;

/** Self-contained physical layout descriptor for one Chunk. */
public record ChunkLayout(
    IDeviceID device,
    boolean aligned,
    long chunkGroupIndex,
    long chunkGroupHeaderOffset,
    boolean firstChunkOfGroup,
    Chunk chunk,
    long offset,
    long length,
    int chunkIndexInGroup) {}
