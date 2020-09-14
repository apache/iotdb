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
package org.apache.iotdb.rpc;


public class Config {
    public enum Constant {
        NUMBER("number"), BOOLEAN("bool");

        Constant(String type) {
            this.type = type;
        }
        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    private Config(){}

    public static Constant boolFormat = Constant.BOOLEAN;
    public static boolean rpcThriftCompressionEnable = false;
    public static int connectionTimeoutInMs = 0;
    public static final int RETRY_NUM = 3;
    public static final long RETRY_INTERVAL = 1000;
    public static int fetchSize = 10000;

    public static void setBoolFormat(Constant boolFormat) {
        Config.boolFormat = boolFormat;
    }

}
