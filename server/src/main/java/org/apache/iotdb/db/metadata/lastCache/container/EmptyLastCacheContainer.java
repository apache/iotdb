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

package org.apache.iotdb.db.metadata.lastCache.container;

import org.apache.iotdb.db.metadata.lastCache.container.value.ILastCacheValue;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
public class EmptyLastCacheContainer implements ILastCacheContainer {

    ILastCacheValue lastCacheValue = null;

    @Override
    public TimeValuePair getCachedLast(){
        return null;
    };

    @Override
    public void updateCachedLast(
            TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime){

    };

    @Override
    public synchronized void resetLastCache() {
        lastCacheValue = null;
    }

    @Override
    public boolean isEmpty() {
        return true;
    };

    @Override
    public boolean isEmptyContainer() {return false;}

}
