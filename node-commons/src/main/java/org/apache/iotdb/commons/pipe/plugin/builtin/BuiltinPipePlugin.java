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

package org.apache.iotdb.commons.pipe.plugin.builtin;

import org.apache.iotdb.commons.pipe.plugin.builtin.connector.DoNothingConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.processor.DoNothingProcessor;

public enum BuiltinPipePlugin {

  // processors
  DO_NOTHING_PROCESSOR("do_nothing_processor", DoNothingProcessor.class),

  // connectors
  DO_NOTHING_CONNECTOR("do_nothing_connector", DoNothingConnector.class),
  ;

  private final String pipePluginName;
  private final Class<?> pipePluginClass;
  private final String className;

  BuiltinPipePlugin(String functionName, Class<?> pipePluginClass) {
    this.pipePluginName = functionName;
    this.pipePluginClass = pipePluginClass;
    this.className = pipePluginClass.getName();
  }

  public String getPipePluginName() {
    return pipePluginName;
  }

  public Class<?> getPipePluginClass() {
    return pipePluginClass;
  }

  public String getClassName() {
    return className;
  }
}
