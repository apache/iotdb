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

package org.apache.iotdb.commons.trigger;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/** This Class used to save the specific information of one Trigger. */
public class TriggerInformation {
  private PartialPath pathPattern;
  private String triggerName;
  private String className;
  private String jarName;

  private Map<String, String> attributes;

  private TTriggerState triggerState;

  /** indicate this Trigger is Stateful or Stateless */
  private boolean isStateful;

  /** only used for Stateful Trigger */
  private TDataNodeLocation dataNodeLocation;

  public TriggerInformation() {};

  public TriggerInformation(
      PartialPath pathPattern,
      String triggerName,
      String className,
      String jarName,
      Map<String, String> attributes,
      TTriggerState triggerState,
      boolean isStateful,
      TDataNodeLocation dataNodeLocation) {
    this.pathPattern = pathPattern;
    this.triggerName = triggerName;
    this.className = className;
    this.jarName = jarName;
    this.attributes = attributes;
    this.triggerState = triggerState;
    this.isStateful = isStateful;
    this.dataNodeLocation = dataNodeLocation;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    pathPattern.serialize(outputStream);
    ReadWriteIOUtils.write(triggerName, outputStream);
    ReadWriteIOUtils.write(className, outputStream);
    ReadWriteIOUtils.write(jarName, outputStream);
    ReadWriteIOUtils.write(attributes, outputStream);
    ReadWriteIOUtils.write(triggerState.getValue(), outputStream);
    ReadWriteIOUtils.write(isStateful, outputStream);
    if (isStateful) {
      ThriftCommonsSerDeUtils.serializeTDataNodeLocation(dataNodeLocation, outputStream);
    }
  }

  public static TriggerInformation deserialize(ByteBuffer byteBuffer) {
    TriggerInformation triggerInformation = new TriggerInformation();
    triggerInformation.pathPattern = PartialPath.deserialize(byteBuffer);
    triggerInformation.triggerName = ReadWriteIOUtils.readString(byteBuffer);
    triggerInformation.className = ReadWriteIOUtils.readString(byteBuffer);
    triggerInformation.jarName = ReadWriteIOUtils.readString(byteBuffer);
    triggerInformation.attributes = ReadWriteIOUtils.readMap(byteBuffer);
    triggerInformation.triggerState =
        TTriggerState.findByValue(ReadWriteIOUtils.readInt(byteBuffer));
    boolean isStateful = ReadWriteIOUtils.readBool(byteBuffer);
    triggerInformation.isStateful = isStateful;
    if (isStateful) {
      triggerInformation.dataNodeLocation =
          ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    }
    return triggerInformation;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public void setPathPattern(PartialPath pathPattern) {
    this.pathPattern = pathPattern;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public String getJarName() {
    return jarName;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public TTriggerState getTriggerState() {
    return triggerState;
  }

  public void setTriggerState(TTriggerState triggerState) {
    this.triggerState = triggerState;
  }

  public boolean isStateful() {
    return isStateful;
  }

  public void setStateful(boolean stateful) {
    isStateful = stateful;
  }

  public TDataNodeLocation getDataNodeLocation() {
    return dataNodeLocation;
  }

  public void setDataNodeLocation(TDataNodeLocation dataNodeLocation) {
    this.dataNodeLocation = dataNodeLocation;
  }
}
