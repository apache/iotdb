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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

public class LoadConfigurationPlan extends PhysicalPlan {

  // an array of properties, the size of which is always 2.
  // The first element is the properties for iotdb-engine, the second
  // is for cluster-config
  private Properties[] propertiesArray;

  LoadConfigurationPlanType loadConfigurationPlanType;

  public LoadConfigurationPlan(
      LoadConfigurationPlanType loadConfigurationPlanType, Properties[] propertiesArray)
      throws QueryProcessException {
    super(false, OperatorType.LOAD_CONFIGURATION);
    if (loadConfigurationPlanType != LoadConfigurationPlanType.GLOBAL) {
      throw new QueryProcessException(
          "The constructor with 2 parameters is for load global configuration");
    }
    if (propertiesArray.length != 2) {
      throw new QueryProcessException("The size of propertiesArray is not 2.");
    }
    this.loadConfigurationPlanType = loadConfigurationPlanType;
    this.propertiesArray = propertiesArray;
  }

  public LoadConfigurationPlan(LoadConfigurationPlanType loadConfigurationPlanType)
      throws QueryProcessException {
    super(false, OperatorType.LOAD_CONFIGURATION);
    if (loadConfigurationPlanType != LoadConfigurationPlanType.LOCAL) {
      throw new QueryProcessException(
          "The constructor with 1 parameters is for load local configuration");
    }
    this.loadConfigurationPlanType = loadConfigurationPlanType;
  }

  // only for deserialize
  public LoadConfigurationPlan() {
    super(false, OperatorType.LOAD_CONFIGURATION);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.LOAD_CONFIGURATION.ordinal();
    stream.writeByte((byte) type);
    stream.writeInt(loadConfigurationPlanType.ordinal());
    if (loadConfigurationPlanType == LoadConfigurationPlanType.GLOBAL) {
      stream.writeInt(propertiesArray.length);
      for (Properties properties : propertiesArray) {
        if (properties == null) {
          stream.writeInt(0);
        } else {
          stream.writeInt(1);
          stream.writeInt(properties.entrySet().size());
          for (Entry<Object, Object> entry : properties.entrySet()) {
            putString(stream, String.valueOf(entry.getKey()));
            putString(stream, String.valueOf(entry.getValue()));
          }
        }
      }
    }

    stream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    loadConfigurationPlanType = LoadConfigurationPlanType.values()[buffer.getInt()];
    if (loadConfigurationPlanType == LoadConfigurationPlanType.GLOBAL) {
      int propertiesNum = buffer.getInt();
      propertiesArray = new Properties[propertiesNum];
      for (int i = 0; i < propertiesArray.length; i++) {
        if (buffer.getInt() == 1) {
          propertiesArray[i] = new Properties();
          int size = buffer.getInt();
          for (int j = 0; j < size; j++) {
            propertiesArray[i].setProperty(readString(buffer), readString(buffer));
          }
        }
      }
    }
    this.index = buffer.getLong();
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return getOperatorType().toString();
  }

  public Properties getIoTDBProperties() {
    return propertiesArray[0];
  }

  public Properties getClusterProperties() {
    return propertiesArray[1];
  }

  public LoadConfigurationPlanType getLoadConfigurationPlanType() {
    return loadConfigurationPlanType;
  }

  public enum LoadConfigurationPlanType {
    GLOBAL,
    LOCAL
  }
}
