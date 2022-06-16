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
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public interface PipeSink {

  // void setAttribute(String attr, String value) throws PipeSinkException;
  void setAttribute(List<Pair<String, String>> params) throws PipeSinkException;

  String getPipeSinkName();

  PipeSinkType getType();

  String showAllAttributes();

  enum PipeSinkType {
    IoTDB,
    ExternalPipe
  }

  class PipeSinkFactory {
    public static PipeSink createPipeSink(String type, String name) {
      type = type.toLowerCase();

      if (PipeSinkType.IoTDB.name().toLowerCase().equals(type)) {
        return new IoTDBPipeSink(name);
      }

      if (ExtPipePluginRegister.getInstance().pluginExist(type)) {
        return new ExternalPipeSink(name, type);
      }

      throw new UnsupportedOperationException(
          String.format("Do not support pipeSink type: %s.", type));
    }
  }
}
