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

package org.apache.iotdb.commons.externalservice;

import org.apache.iotdb.externalservice.api.IExternalService;

import com.google.common.base.Objects;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ServiceInfo {
  private final String serviceName;
  private final String className;
  // This field needn't to serde, only USER_DEFINED service will be persisted on CN
  private final transient ServiceType serviceType;
  private State state;

  private transient IExternalService serviceInstance;

  public ServiceInfo(String serviceName, String className, ServiceType serviceType) {
    this.serviceName = serviceName;
    this.className = className;
    this.serviceType = serviceType;
    this.state = State.STOPPED;
  }

  public ServiceInfo(String serviceName, String className, ServiceType serviceType, State state) {
    this.serviceName = serviceName;
    this.className = className;
    this.serviceType = serviceType;
    this.state = state;
  }

  public String getClassName() {
    return className;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public ServiceType getServiceType() {
    return serviceType;
  }

  public String getServiceName() {
    return serviceName;
  }

  public IExternalService getServiceInstance() {
    return serviceInstance;
  }

  public void setServiceInstance(IExternalService serviceInstance) {
    this.serviceInstance = serviceInstance;
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(serviceName, stream);
    ReadWriteIOUtils.write(className, stream);
    ReadWriteIOUtils.write(state.getValue(), stream);
  }

  public static ServiceInfo deserialize(ByteBuffer buffer) {
    String serviceName = ReadWriteIOUtils.readString(buffer);
    String className = ReadWriteIOUtils.readString(buffer);
    State state = State.deserialize(ReadWriteIOUtils.readByte(buffer));
    return new ServiceInfo(serviceName, className, ServiceType.USER_DEFINED, state);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServiceInfo that = (ServiceInfo) o;
    return Objects.equal(serviceName, that.serviceName)
        && Objects.equal(className, that.className)
        && serviceType == that.serviceType
        && state == that.state;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(serviceName, className, serviceType, state);
  }

  public enum ServiceType {
    BUILTIN((byte) 0, "built-in"),
    USER_DEFINED((byte) 1, "user-defined");

    private final byte value;

    private final String displayName;

    ServiceType(byte value, String displayName) {
      this.value = value;
      this.displayName = displayName;
    }

    public byte getValue() {
      return value;
    }

    public String getDisplayName() {
      return displayName;
    }

    public static ServiceType deserialize(byte t) {
      switch (t) {
        case 0:
          return BUILTIN;
        case 1:
          return USER_DEFINED;
        default:
          throw new IllegalArgumentException("Unknown ServiceType: " + t);
      }
    }
  }

  public enum State {
    RUNNING((byte) 0),
    STOPPED((byte) 1);

    private final byte value;

    State(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return value;
    }

    public static State deserialize(byte t) {
      switch (t) {
        case 0:
          return RUNNING;
        case 1:
          return STOPPED;
        default:
          throw new IllegalArgumentException("Unknown State: " + t);
      }
    }
  }
}
