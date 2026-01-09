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

package org.apache.iotdb.db.service.exernalservice;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.externalservice.api.IExternalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.externalservice.ServiceInfo.State.RUNNING;
import static org.apache.iotdb.commons.externalservice.ServiceInfo.State.STOPPED;

public class ExternalServiceManagementService {
  @GuardedBy("lock")
  private final Map<String, ServiceInfo> serviceInfos;

  private final String libRoot;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExternalServiceManagementService.class);

  private ExternalServiceManagementService(String libRoot) {
    this.serviceInfos = new HashMap<>();
    this.libRoot = libRoot;
  }

  public List<ServiceInfo> getAllServices() {
    try {
      lock.writeLock().lock();
      return serviceInfos.values().stream().collect(Collectors.toList());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void createService(String serviceName, String className) {
    try {
      lock.writeLock().lock();

      if (serviceInfos.containsKey(serviceName)) {
        throw new ExternalServiceManagementException(
            String.format("Failed to create External Service %s, it already exists!", serviceName));
      }

      /*logAccessor.writeWal(
      new ServiceLogAccessor.ServiceWalEntry(
          ServiceLogAccessor.OperationType.CREATE, serviceName, className));*/
      serviceInfos.put(
          serviceName,
          new ServiceInfo(serviceName, className, ServiceInfo.ServiceType.USER_DEFINED));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void startService(String serviceName) {
    try {
      lock.writeLock().lock();

      ServiceInfo serviceInfo = serviceInfos.get(serviceName);
      if (serviceInfo == null) {
        throw new ExternalServiceManagementException(
            String.format(
                "Failed to stop External Service %s, because it is not existed!", serviceName));
      }

      if (serviceInfo.getState() == RUNNING) {
        return;
      } else {
        // The state is STOPPED
        if (serviceInfo.getServiceInstance() != null) {
          serviceInfo.getServiceInstance().start();
        } else {
          // lazy create Instance
          serviceInfo.setServiceInstance(createExternalServiceInstance(serviceName));
        }
      }

      /*logAccessor.writeWal(
      new ServiceLogAccessor.ServiceWalEntry(
          ServiceLogAccessor.OperationType.START, serviceName, null));*/
      serviceInfo.setState(RUNNING);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private IExternalService createExternalServiceInstance(String serviceName) {
    // close ClassLoader automatically to release the file handle
    try (ExternalServiceClassLoader classLoader = new ExternalServiceClassLoader(libRoot); ) {
      return (IExternalService)
          Class.forName(serviceName, true, classLoader).getDeclaredConstructor().newInstance();
    } catch (IOException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException
        | ClassCastException e) {
      String errorMessage =
          String.format(
              "Failed to start External Service %s, because its instance can not be constructed successfully. Exception: %s",
              serviceName, e);
      LOGGER.warn(errorMessage, e);
      throw new ExternalServiceManagementException(errorMessage);
    }
  }

  public void stopService(String serviceName) {
    try {
      lock.writeLock().lock();

      ServiceInfo serviceInfo = serviceInfos.get(serviceName);
      if (serviceInfo == null) {
        throw new ExternalServiceManagementException(
            String.format(
                "Failed to start External Service %s, because it is not existed!", serviceName));
      }

      if (serviceInfo.getState() == STOPPED) {
        return;
      } else {
        // The state is RUNNING
        stopService(serviceInfo);
      }

      /*logAccessor.writeWal(
      new ServiceLogAccessor.ServiceWalEntry(
          ServiceLogAccessor.OperationType.STOP, serviceName, null));*/
      serviceInfo.setState(STOPPED);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void stopService(ServiceInfo serviceInfo) {
    checkState(
        serviceInfo.getServiceInstance() != null,
        "External Service instance is null when state is RUNNING!",
        serviceInfo.getServiceName());
    serviceInfo.getServiceInstance().stop();
  }

  public void dropService(String serviceName, boolean forcedly) {
    try {
      lock.writeLock().lock();

      ServiceInfo serviceInfo = serviceInfos.get(serviceName);
      if (serviceInfo == null) {
        throw new ExternalServiceManagementException(
            String.format(
                "Failed to drop External Service %s, because it is not existed!", serviceName));
      }
      if (serviceInfo.getServiceType() == ServiceInfo.ServiceType.BUILTIN) {
        throw new ExternalServiceManagementException(
            String.format(
                "Failed to drop External Service %s, because it is BUILT-IN!", serviceName));
      }

      if (serviceInfo.getState() == STOPPED) {
        // do nothing
      } else {
        // The state is RUNNING
        if (forcedly) {
          try {
            stopService(serviceInfo);
          } catch (Exception e) {
            // record errMsg if exception occurs during the stop of service
            LOGGER.warn(
                "Failed to stop External Service %s because %s. It will be drop forcedly",
                serviceName, e.getMessage());
          }
        } else {
          throw new ExternalServiceManagementException(
              String.format(
                  "Failed to drop External Service %s, because it is RUNNING!", serviceName));
        }
      }

      /*logAccessor.writeWal(
      new ServiceLogAccessor.ServiceWalEntry(
          ServiceLogAccessor.OperationType.DROP, serviceName, null));*/
      serviceInfos.remove(serviceName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void start() throws StartupException {
    lock.writeLock().lock();
    try {
      restoreBuiltInServices();
      restoreUserDefinedServices();

      // start services with RUNNING state
      serviceInfos
          .values()
          .forEach(
              serviceInfo -> {
                if (serviceInfo.getState() == RUNNING) {
                  serviceInfo.setServiceInstance(
                      createExternalServiceInstance(serviceInfo.getServiceName()));
                }
              });
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void restoreBuiltInServices() {
    for (BuiltinExternalServices builtinExternalService : BuiltinExternalServices.values()) {
      serviceInfos.put(
          builtinExternalService.getServiceName(),
          new ServiceInfo(
              builtinExternalService.getServiceName(),
              builtinExternalService.getClassName(),
              ServiceInfo.ServiceType.BUILTIN,
              builtinExternalService.isEnabled() ? RUNNING : STOPPED));
    }
  }

  private void restoreUserDefinedServices() {
    // TODO
  }

  public static ExternalServiceManagementService getInstance() {
    return ExternalServiceManagementServiceHolder.INSTANCE;
  }

  private static class ExternalServiceManagementServiceHolder {

    private static final ExternalServiceManagementService INSTANCE =
        new ExternalServiceManagementService(
            IoTDBDescriptor.getInstance().getConfig().getExternalServiceDir());

    private ExternalServiceManagementServiceHolder() {}
  }
}
