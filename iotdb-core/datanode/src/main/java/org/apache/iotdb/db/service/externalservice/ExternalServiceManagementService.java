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

package org.apache.iotdb.db.service.externalservice;

import org.apache.iotdb.common.rpc.thrift.TExternalServiceEntry;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateExternalServiceReq;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.externalservice.api.IExternalService;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
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
    restoreBuiltInServices();
    this.libRoot = libRoot;
  }

  public Iterator<TExternalServiceEntry> showService(int dataNodeId)
      throws ClientManagerException, TException {
    try {
      lock.readLock().lock();

      try (ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        TExternalServiceListResp resp = client.showExternalService(dataNodeId);
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new IoTDBRuntimeException(resp.getStatus().message, resp.getStatus().code);
        }
        return resp.getExternalServiceInfos().iterator();
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void createService(String serviceName, String className)
      throws ClientManagerException, TException {
    try {
      lock.writeLock().lock();

      // 1. validate
      if (serviceInfos.containsKey(serviceName)) {
        TSStatus status = new TSStatus(TSStatusCode.EXTERNAL_SERVICE_ALREADY_EXIST.getStatusCode());
        status.setMessage(
            String.format("Failed to create External Service %s, it already exists!", serviceName));
        throw new ExternalServiceManagementException(status);
      }

      // 2. persist on CN
      try (ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        TSStatus status =
            client.createExternalService(
                new TCreateExternalServiceReq(QueryId.getDataNodeId(), serviceName, className));
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new IoTDBRuntimeException(status.message, status.code);
        }
      }

      // 3. modify memory info
      serviceInfos.put(
          serviceName,
          new ServiceInfo(serviceName, className, ServiceInfo.ServiceType.USER_DEFINED));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void startService(String serviceName) throws ClientManagerException, TException {
    try {
      lock.writeLock().lock();

      // 1. validate
      ServiceInfo serviceInfo = serviceInfos.get(serviceName);
      if (serviceInfo == null) {
        TSStatus status = new TSStatus(TSStatusCode.NO_SUCH_EXTERNAL_SERVICE.getStatusCode());
        status.setMessage(
            String.format(
                "Failed to start External Service %s, because it is not existed!", serviceName));
        throw new ExternalServiceManagementException(status);
      }

      // 2. call start method of ServiceInstance, create if Instance was not created
      if (serviceInfo.getState() == RUNNING) {
        return;
      } else {
        // The state is STOPPED
        if (serviceInfo.getServiceInstance() == null) {
          // lazy create Instance
          serviceInfo.setServiceInstance(
              createExternalServiceInstance(serviceName, serviceInfo.getClassName()));
        }
        serviceInfo.getServiceInstance().start();
      }

      // 3. persist on CN if service is user-defined, rollback if failed
      if ((serviceInfo.getServiceType() == ServiceInfo.ServiceType.USER_DEFINED)) {
        try (ConfigNodeClient client =
            ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
          TSStatus status = client.startExternalService(QueryId.getDataNodeId(), serviceName);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            serviceInfo.getServiceInstance().stop();
            throw new IoTDBRuntimeException(status.message, status.code);
          }
        }
      }

      // 4. modify memory info
      serviceInfo.setState(RUNNING);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private IExternalService createExternalServiceInstance(String serviceName, String className) {
    // close ClassLoader automatically to release the file handle
    try (ExternalServiceClassLoader classLoader = new ExternalServiceClassLoader(libRoot); ) {
      return (IExternalService)
          Class.forName(className, true, classLoader).getDeclaredConstructor().newInstance();
    } catch (IOException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException
        | ClassCastException e) {
      TSStatus status =
          new TSStatus(TSStatusCode.EXTERNAL_SERVICE_INSTANCE_CREATE_ERROR.getStatusCode());
      status.setMessage(
          String.format(
              "Failed to start External Service %s, because its instance can not be constructed successfully. Exception: %s",
              serviceName, e));
      LOGGER.warn(status.getMessage(), e);
      throw new ExternalServiceManagementException(status);
    }
  }

  public void stopService(String serviceName) throws ClientManagerException, TException {
    try {
      lock.writeLock().lock();

      // 1. validate
      ServiceInfo serviceInfo = serviceInfos.get(serviceName);
      if (serviceInfo == null) {
        TSStatus status = new TSStatus(TSStatusCode.NO_SUCH_EXTERNAL_SERVICE.getStatusCode());
        status.setMessage(
            String.format(
                "Failed to stop External Service %s, because it is not existed!", serviceName));
        throw new ExternalServiceManagementException(status);
      }

      // 2. call stop method of ServiceInstance
      if (serviceInfo.getState() == STOPPED) {
        return;
      } else {
        // The state is RUNNING
        stopService(serviceInfo);
      }

      // 3. persist on CN if service is user-defined, rollback if failed
      if ((serviceInfo.getServiceType() == ServiceInfo.ServiceType.USER_DEFINED)) {
        try (ConfigNodeClient client =
            ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID); ) {
          TSStatus status = client.stopExternalService(QueryId.getDataNodeId(), serviceName);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            serviceInfo.getServiceInstance().start();
            throw new IoTDBRuntimeException(status.message, status.code);
          }
        }
      }

      // 4. modify memory info
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

  public void dropService(String serviceName, boolean forcedly)
      throws ClientManagerException, TException {
    try {
      lock.writeLock().lock();

      // 1. validate
      ServiceInfo serviceInfo = serviceInfos.get(serviceName);
      if (serviceInfo == null) {
        TSStatus status = new TSStatus(TSStatusCode.NO_SUCH_EXTERNAL_SERVICE.getStatusCode());
        status.setMessage(
            String.format(
                "Failed to drop External Service %s, because it is not existed!", serviceName));
        throw new ExternalServiceManagementException(status);
      }
      if (serviceInfo.getServiceType() == ServiceInfo.ServiceType.BUILTIN) {
        TSStatus status =
            new TSStatus(TSStatusCode.CANNOT_DROP_BUILTIN_EXTERNAL_SERVICE.getStatusCode());
        status.setMessage(
            String.format(
                "Failed to drop External Service %s, because it is BUILT-IN!", serviceName));
        throw new ExternalServiceManagementException(status);
      }

      // 2. stop or fail when service are not stopped
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
          TSStatus status =
              new TSStatus(TSStatusCode.CANNOT_DROP_RUNNING_EXTERNAL_SERVICE.getStatusCode());
          status.setMessage(
              String.format(
                  "Failed to drop External Service %s, because it is RUNNING!", serviceName));
          throw new ExternalServiceManagementException(status);
        }
      }

      // 3. persist on CN
      try (ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        TSStatus status = client.dropExternalService(QueryId.getDataNodeId(), serviceName);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new IoTDBRuntimeException(status.message, status.code);
        }
      }

      // 4. modify memory info
      serviceInfos.remove(serviceName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void restoreRunningServiceInstance() {
    // Needn't use lock here, we use this method when active DN and there is no concurrent risk
    serviceInfos
        .values()
        .forEach(
            serviceInfo -> {
              // start services with RUNNING state
              if (serviceInfo.getState() == RUNNING) {
                IExternalService serviceInstance =
                    createExternalServiceInstance(
                        serviceInfo.getServiceName(), serviceInfo.getClassName());
                serviceInfo.setServiceInstance(serviceInstance);
                serviceInstance.start();
              }
            });
  }

  public void restoreUserDefinedServices(List<TExternalServiceEntry> serviceEntryList) {
    // Needn't use lock here, we use this method when active DN and there is no concurrent risk
    for (TExternalServiceEntry serviceEntry : serviceEntryList) {
      serviceInfos.put(
          serviceEntry.getServiceName(),
          new ServiceInfo(
              serviceEntry.getServiceName(),
              serviceEntry.getClassName(),
              ServiceInfo.ServiceType.USER_DEFINED,
              ServiceInfo.State.deserialize(serviceEntry.getState())));
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

  public List<TExternalServiceEntry> getBuiltInServices() {
    try {
      lock.readLock().lock();
      return serviceInfos.values().stream()
          .filter(serviceInfo -> serviceInfo.getServiceType() == ServiceInfo.ServiceType.BUILTIN)
          .map(
              serviceInfo ->
                  new TExternalServiceEntry(
                      serviceInfo.getServiceName(),
                      serviceInfo.getClassName(),
                      serviceInfo.getState().getValue(),
                      QueryId.getDataNodeId(),
                      ServiceInfo.ServiceType.BUILTIN.getValue()))
          .collect(Collectors.toList());
    } finally {
      lock.readLock().unlock();
    }
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
