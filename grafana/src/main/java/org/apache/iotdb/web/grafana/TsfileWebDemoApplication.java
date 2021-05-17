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
package org.apache.iotdb.web.grafana;

import org.apache.iotdb.jdbc.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Repository;

@SpringBootApplication
@Repository
@PropertySource("classpath:application.properties")
public class TsfileWebDemoApplication {

  @Value("${rpc_thrift_compression_enable}")
  private static boolean rpcThriftCompressionEnable;

  public static void main(String[] args) {
    Config.rpcThriftCompressionEnable = rpcThriftCompressionEnable;
    SpringApplication.run(TsfileWebDemoApplication.class, args);
  }
}
