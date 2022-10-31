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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable;

import org.roaringbitmap.RoaringBitmap;

/** used to manage the device id collection */
public class MemChunk {

  // manage the device id collection, see: https://github.com/RoaringBitmap/RoaringBitmap
  private RoaringBitmap roaringBitmap;

  public MemChunk() {
    roaringBitmap = new RoaringBitmap();
  }

  public boolean isEmpty() {
    if (roaringBitmap == null) return true;
    return roaringBitmap.isEmpty();
  }

  @Override
  public String toString() {
    return roaringBitmap.toString();
  }

  public void put(int id) {
    roaringBitmap.add(id);
  }

  public void remove(int id) {
    roaringBitmap.remove(id);
  }

  public RoaringBitmap getRoaringBitmap() {
    return this.roaringBitmap;
  }
}
