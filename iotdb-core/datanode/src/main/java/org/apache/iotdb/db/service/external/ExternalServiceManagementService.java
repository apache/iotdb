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

package org.apache.iotdb.db.service.external;

import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.external.ServiceExecutableManager;
import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.service.external.ServiceStatus;
import org.apache.iotdb.commons.service.external.ServiceTable;
import org.apache.iotdb.commons.service.external.exception.ServiceManagementException;
import org.apache.iotdb.service.api.IExternalService;
import org.apache.iotdb.service.api.ServiceState;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class ExternalServiceManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterManager.class);

  private static final long deregisterTimeOut = 10_000L;

  private final Map<String, IExternalService> iExternalServices;

  private final ReentrantLock lock;

  private final ServiceTable serviceTable;

  private ExternalServiceManagementService() {
    iExternalServices = new HashMap<>();
    lock = new ReentrantLock();
    serviceTable = new ServiceTable();
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public void register(ServiceInformation serviceInformation, ByteBuffer jarFile)
      throws IOException {
    try {
      acquireLock();
      checkIfRegistered(serviceInformation);
      saveJarFile(serviceInformation.getJarName(), jarFile);
      doRegister(serviceInformation);
    } finally {
      releaseLock();
    }
  }

  public void doRegister(ServiceInformation serviceInformation) throws IOException {
    try (ExternalServiceClassLoader externalServiceClassLoader =
        ExternalServiceClassLoaderManager.getInstance().updateAndGetActiveClassLoader()) {
      String serviceName = serviceInformation.getServiceName();
      serviceTable.addServiceInformation(serviceName, serviceInformation);
      IExternalService iExternalService =
          constructService(serviceInformation.getClassName(), externalServiceClassLoader);
      iExternalServices.put(serviceName, iExternalService);
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Failed to register service %s with className: %s. The cause is: %s",
              serviceInformation.getServiceName(), serviceInformation.getClassName(), e);
      LOGGER.warn(errorMessage);
      throw e;
    }
  }

  public void dropService(String serviceName, Boolean needToDeleteJar)
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    try {
      acquireLock();
      if (iExternalServices.containsKey(serviceName)) {
        IExternalService iExternalService = iExternalServices.get(serviceName);
        if (iExternalService != null
            && ServiceState.isRunning(iExternalService.getServiceState())) {
          CompletableFuture<Void> future = CompletableFuture.runAsync(iExternalService::stop);
          future.get(deregisterTimeOut, TimeUnit.MILLISECONDS);

          iExternalServices.remove(serviceName);
          serviceTable.removeServiceInformation(serviceName);
          if (needToDeleteJar) {
            ServiceExecutableManager.getInstance()
                .removeFileUnderLibRoot(
                    serviceTable.getServiceInformation(serviceName).getJarName());
          }
        }
      }
    } catch (IOException | ExecutionException | InterruptedException | TimeoutException e) {
      String errorMessage =
          String.format("Failed to drop service %s. The cause is: %s", serviceName, e.getMessage());
      LOGGER.warn(errorMessage);
      throw e;
    } finally {
      releaseLock();
    }
  }

  public void startService(String serviceName) {
    try {
      acquireLock();
      if (iExternalServices.containsKey(serviceName)) {
        IExternalService iExternalService = iExternalServices.get(serviceName);
        if (iExternalService != null
            && !ServiceState.isRunning(iExternalService.getServiceState())) {
          CompletableFuture.runAsync(
              () -> {
                try {
                  iExternalService.start();
                } catch (Exception e) {
                  LOGGER.error("Failed to start service: {}", serviceName, e);
                }
              });
        }
      }
    } finally {
      serviceTable.getServiceInformation(serviceName).setServiceStatus(ServiceStatus.ACTIVE);
      releaseLock();
    }
  }

  public void stopService(String serviceName)
      throws ExecutionException, InterruptedException, TimeoutException {
    try {
      acquireLock();
      if (iExternalServices.containsKey(serviceName)) {
        IExternalService iExternalService = iExternalServices.get(serviceName);
        if (iExternalService != null) {
          CompletableFuture<Void> future = CompletableFuture.runAsync(iExternalService::stop);
          future.get(deregisterTimeOut, TimeUnit.MILLISECONDS);
        }
      }
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      String errorMessage =
          String.format("Failed to stop service %s. The cause is: %s", serviceName, e.getMessage());
      LOGGER.warn(errorMessage);
      throw e;
    } finally {
      serviceTable.getServiceInformation(serviceName).setServiceStatus(ServiceStatus.INACTIVE);
      releaseLock();
    }
  }

  public Optional<ServiceState> getServiceState(String serviceName) {
    try {
      acquireLock();
      if (iExternalServices.containsKey(serviceName)) {
        IExternalService iExternalService = iExternalServices.get(serviceName);
        if (iExternalService != null) {
          return Optional.of(iExternalService.getServiceState());
        }
      }
      return Optional.empty();
    } finally {
      releaseLock();
    }
  }

  private IExternalService constructService(
      String className, ExternalServiceClassLoader classLoader) {
    try {
      Class<?> serviceClass = Class.forName(className, true, classLoader);
      return (IExternalService) serviceClass.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new ServiceManagementException(
          String.format(
              "Failed to reflect service instance with className(%s), because %s", className, e));
    }
  }

  private void saveJarFile(String jarName, ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer != null) {
      ServiceExecutableManager.getInstance().saveToInstallDir(byteBuffer, jarName);
    }
  }

  private void checkIfRegistered(ServiceInformation serviceInformation)
      throws ServiceManagementException {
    String serviceName = serviceInformation.getServiceName();
    String jarName = serviceInformation.getJarName();
    if (serviceTable.containsService(serviceName)
        && ServiceExecutableManager.getInstance().hasFileUnderLibRoot(jarName)
        && isLocalJarConflicted(serviceInformation)) {
      // same jar name with different md5
      String errorMessage =
          String.format(
              "Failed to registered service %s, because existed"
                  + " md5 of jar file for service %s is different from the new jar file. ",
              serviceName, serviceName);
      LOGGER.warn(errorMessage);
      throw new ServiceManagementException(errorMessage);
    }
  }

  public boolean isLocalJarConflicted(ServiceInformation serviceInformation) {
    String serviceName = serviceInformation.getServiceName();
    String existedMd5 = "";
    String md5FilePath = serviceName + ".txt";

    // if meet error when reading md5 from txt, we need to compute it again
    boolean hasComputed = false;
    if (ServiceExecutableManager.getInstance().hasFileUnderTemporaryRoot(md5FilePath)) {
      try {
        existedMd5 =
            ServiceExecutableManager.getInstance().readTextFromFileUnderTemporaryRoot(md5FilePath);
        hasComputed = true;
      } catch (IOException e) {
        LOGGER.warn("Error occurred when trying to read md5 of {}", md5FilePath);
      }
    }
    if (!hasComputed) {
      try {
        existedMd5 =
            DigestUtils.md5Hex(
                Files.newInputStream(
                    Paths.get(
                        ServiceExecutableManager.getInstance().getInstallDir()
                            + File.separator
                            + serviceInformation.getJarName())));
        // save the md5 in a txt under service temporary lib
        ServiceExecutableManager.getInstance()
            .saveTextAsFileUnderTemporaryRoot(existedMd5, md5FilePath);
      } catch (IOException e) {
        String errorMessage =
            String.format(
                "Failed to registered serivce %s, because error "
                    + "occurred when trying to compute md5 of jar file for service %s ",
                serviceName, serviceName);
        LOGGER.warn(errorMessage, e);
        throw new ServiceManagementException(errorMessage);
      }
    }
    return !existedMd5.equals(serviceInformation.getJarFileMD5());
  }

  public void logServiceStates() {
    for (Map.Entry<String, IExternalService> entry : iExternalServices.entrySet()) {
      String serviceName = entry.getKey();
      IExternalService service = entry.getValue();
      if (service != null) {
        LOGGER.info("External Service: {}, State: {}", serviceName, service.getServiceState());
      } else {
        LOGGER.info("External Service: {}, State: null", serviceName);
      }
    }
  }

  private static class SingletonHolder {
    private static final ExternalServiceManagementService INSTANCE =
        new ExternalServiceManagementService();
  }

  public static ExternalServiceManagementService getInstance() {
    return SingletonHolder.INSTANCE;
  }
}
