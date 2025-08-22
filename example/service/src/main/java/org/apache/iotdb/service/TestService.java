package org.apache.iotdb.service;

import org.apache.iotdb.service.api.IExternalService;
import org.apache.iotdb.service.api.ServiceState;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class TestService implements IExternalService {
  private static final String SERVICE_PATH_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator
          + "service"
          + File.separator;
  private static final String CONF_FILE_NAME = "conf.properties";
  private static final long stopWaitTime = 1000;
  private static final int defaultPort = 5555;

  private final ReentrantLock reentrantLock = new ReentrantLock();
  AtomicBoolean shouldStop;
  private AtomicReference<ServiceState> serviceState;
  private CompletableFuture<Void> serviceStateFuture;
  private int port;

  public TestService() throws IOException {
    this.serviceState = new AtomicReference<>();
    this.serviceState.set(ServiceState.STOPPED);
    this.shouldStop = new AtomicBoolean(false);
    this.serviceStateFuture = CompletableFuture.completedFuture(null);
    loadConfig();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    TestService testService = new TestService();
    testService.start();
    Thread.sleep(2000);
    testService.stop();
  }

  private void loadConfig() throws IOException {
    this.port = defaultPort;
    File file = new File(SERVICE_PATH_PREFIX + CONF_FILE_NAME);
    if (file.exists()) {
      try (FileInputStream fis = new FileInputStream(file)) {
        Properties prop = new Properties();
        prop.load(fis);
        String portStr = prop.getProperty("port");
        if (portStr != null && !portStr.trim().isEmpty()) {
          this.port = Integer.parseInt(portStr);
        }
      } catch (Exception e) {
        System.out.println("Error loading config file, using default port " + defaultPort);
        System.out.println(e.getMessage());
      }
    }
  }

  @Override
  public void start() {
    reentrantLock.lock();
    try {
      shouldStop.set(false);
      serviceState.set(ServiceState.STARTING);
      System.out.println("Service is starting on port " + port);
      serviceStateFuture =
          CompletableFuture.runAsync(
              () -> {
                try (ServerSocket serverSocket = new ServerSocket()) {
                  serverSocket.setReuseAddress(true);
                  serverSocket.bind(new InetSocketAddress(port));
                  serverSocket.setSoTimeout(500);
                  while (!shouldStop.get()) {
                    try (Socket clientSocket = serverSocket.accept()) {
                      handleRequest(clientSocket);
                    } catch (SocketTimeoutException e) {
                      // Ignore and continue to check shouldStop
                    }
                  }
                } catch (Exception e) {
                  System.out.println(e.getMessage());
                  serviceState.set(ServiceState.FAILED);
                }
              });
      serviceState.compareAndSet(ServiceState.STARTING, ServiceState.RUNNING);
    } finally {
      reentrantLock.unlock();
    }
  }

  private void handleRequest(Socket clientSocket) throws IOException {
    try (PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
      out.println("HTTP/1.1 200 OK");
      out.println("Content-Type: text/html");
      out.println();
      out.println("Hello!");
      clientSocket.close();
    }
  }

  @Override
  public void stop() {
    reentrantLock.lock();
    serviceState.set(ServiceState.STOPPING);
    shouldStop.set(true);
    try {
      serviceStateFuture.get(stopWaitTime, java.util.concurrent.TimeUnit.MILLISECONDS);
      serviceState.set(ServiceState.STOPPED);
      System.out.println("Service stopped successfully.");
    } catch (Exception e) {
      System.out.println("Error while stopping the service");
      e.printStackTrace(System.out);
      serviceState.set(ServiceState.FAILED);
    } finally {
      reentrantLock.unlock();
    }
  }

  @Override
  public boolean isRunning() {
    return ServiceState.isRunning(serviceState.get());
  }

  @Override
  public ServiceState getServiceState() {
    return serviceState.get();
  }
}
