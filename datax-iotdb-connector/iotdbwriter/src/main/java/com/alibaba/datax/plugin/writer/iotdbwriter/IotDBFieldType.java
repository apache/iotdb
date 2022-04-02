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

package com.alibaba.datax.plugin.writer.iotdbwriter;

public enum  IotDBFieldType {
    /** BOOLEAN */
    BOOLEAN,

    /** INT32 */
    INT32,

    /** INT64 */
    INT64,

    /** FLOAT */
    FLOAT,

    /** DOUBLE */
    DOUBLE,

    /** TEXT */
    TEXT;

    public static IotDBFieldType getIotDBFieldType(String type) {
        if (type == null) {
            return null;
        }
        for (IotDBFieldType f : IotDBFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
