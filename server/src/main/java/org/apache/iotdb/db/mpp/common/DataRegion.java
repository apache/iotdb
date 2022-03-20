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

package org.apache.iotdb.db.mpp.common;

/**
 * This class is used to represent the data partition info including the DataRegionId and physical node IP address
 */
//TODO: (xingtanzjr) This class should be substituted with the class defined in Consensus level
public class DataRegion {
    private Integer dataRegionId;
    private String endpoint;

    public DataRegion(Integer dataRegionId, String endpoint) {
        this.dataRegionId = dataRegionId;
        this.endpoint = endpoint;
    }

    public int hashCode() {
        return dataRegionId.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof DataRegion) {
            return this.dataRegionId.equals(((DataRegion)obj).dataRegionId);
        }
        return false;
    }

    public String toString() {
        return String.format("%s/%d",this.endpoint, this.dataRegionId);
    }
}
