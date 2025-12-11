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

package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import org.apache.iotdb.pipe.api.exception.PipeException;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** ScpFileTransfer - Transfer files to remote server via SCP using JSch library */
public class ScpFileTransfer implements FileTransfer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScpFileTransfer.class);

  private final String host;
  private final int port;
  private final String user;
  private final String password;
  private final String remotePath;

  private JSch jsch;
  private Session session;

  public ScpFileTransfer(
      final String host,
      final int port,
      final String user,
      final String password,
      final String remotePath) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.remotePath = remotePath;
    this.jsch = new JSch();
  }

  /** Get or create SSH session */
  private Session getSession() throws JSchException {
    if (session == null || !session.isConnected()) {
      session = jsch.getSession(user, host, port);
      session.setPassword(password);

      // Set SSH configuration
      final Properties config = new Properties();
      config.put("StrictHostKeyChecking", "no");
      config.put("PreferredAuthentications", "password");
      session.setConfig(config);

      // Set connection timeout
      session.setTimeout(10000);

      session.connect();
      LOGGER.debug("SSH session connected to {}@{}:{}", user, host, port);
    }
    return session;
  }

  /** Test SCP connection */
  public void testConnection() throws Exception {
    try {
      final Session testSession = getSession();
      if (testSession.isConnected()) {
        LOGGER.info("SCP connection test successful: {}@{}:{}", user, host, port);
      } else {
        throw new PipeException(String.format("Failed to connect to %s@%s:%d", user, host, port));
      }
    } catch (final JSchException e) {
      throw new PipeException(
          String.format(
              "Failed to test SCP connection to %s@%s:%d: %s", user, host, port, e.getMessage()),
          e);
    }
  }

  /**
   * Transfer file to remote server
   *
   * @param localFile local file
   * @throws Exception throws exception when transfer fails
   */
  public void transferFile(final File localFile) throws Exception {
    if (!localFile.exists()) {
      throw new PipeException("Local file does not exist: " + localFile);
    }

    if (localFile.isDirectory()) {
      transferDirectory(localFile);
      return;
    }

    final Session currentSession = getSession();
    Channel channel = null;
    FileInputStream fileInputStream = null;

    try {
      // Create SCP channel
      final String command = String.format("scp -t %s", remotePath);
      channel = currentSession.openChannel("exec");
      ((ChannelExec) channel).setCommand(command);

      // Get input/output streams
      final InputStream inputStream = channel.getInputStream();
      final java.io.OutputStream outputStream = channel.getOutputStream();

      channel.connect();

      // Check if remote command succeeded
      checkAck(inputStream);

      // Send file information
      final long fileSize = localFile.length();
      final String fileCommand = String.format("C0644 %d %s\n", fileSize, localFile.getName());
      outputStream.write(fileCommand.getBytes());
      outputStream.flush();
      checkAck(inputStream);

      // Transfer file content
      fileInputStream = new FileInputStream(localFile);
      final byte[] buffer = new byte[1024];
      long totalBytes = 0;
      while (totalBytes < fileSize) {
        final int bytesRead =
            fileInputStream.read(buffer, 0, (int) Math.min(buffer.length, fileSize - totalBytes));
        if (bytesRead < 0) {
          break;
        }
        outputStream.write(buffer, 0, bytesRead);
        totalBytes += bytesRead;
      }

      // Send end marker
      outputStream.write(0);
      outputStream.flush();
      checkAck(inputStream);

      LOGGER.info(
          "Successfully transferred file {} to {}@{}:{}", localFile, user, host, remotePath);
    } catch (final Exception e) {
      throw new PipeException(
          String.format(
              "Failed to transfer file %s to %s@%s:%s: %s",
              localFile, user, host, remotePath, e.getMessage()),
          e);
    } finally {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (final IOException e) {
          LOGGER.warn("Failed to close file input stream", e);
        }
      }
      if (channel != null) {
        channel.disconnect();
      }
    }
  }

  /**
   * Transfer directory to remote server
   *
   * @param localDir local directory
   * @throws Exception throws exception when transfer fails
   */
  public void transferDirectory(final File localDir) throws Exception {
    if (!localDir.exists() || !localDir.isDirectory()) {
      throw new PipeException("Local directory does not exist or is not a directory: " + localDir);
    }

    final Session currentSession = getSession();
    Channel channel = null;

    try {
      // Create SCP channel for directory transfer
      final String command = String.format("scp -r -t %s", remotePath);
      channel = currentSession.openChannel("exec");
      ((ChannelExec) channel).setCommand(command);

      final InputStream inputStream = channel.getInputStream();
      final java.io.OutputStream outputStream = channel.getOutputStream();

      channel.connect();
      checkAck(inputStream);

      // Recursively transfer directory
      transferDirectoryRecursive(localDir, localDir.getName(), outputStream, inputStream);

      LOGGER.info(
          "Successfully transferred directory {} to {}@{}:{}", localDir, user, host, remotePath);
    } catch (final Exception e) {
      throw new PipeException(
          String.format(
              "Failed to transfer directory %s to %s@%s:%s: %s",
              localDir, user, host, remotePath, e.getMessage()),
          e);
    } finally {
      if (channel != null) {
        channel.disconnect();
      }
    }
  }

  /** Recursively transfer directory */
  private void transferDirectoryRecursive(
      final File localDir,
      final String remoteDirName,
      final java.io.OutputStream outputStream,
      final InputStream inputStream)
      throws Exception {
    // Create remote directory
    final String dirCommand = String.format("D0755 0 %s\n", remoteDirName);
    outputStream.write(dirCommand.getBytes());
    outputStream.flush();
    checkAck(inputStream);

    // Transfer files in directory
    final File[] files = localDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        if (file.isFile()) {
          transferFileInDirectory(file, file.getName(), outputStream, inputStream);
        } else if (file.isDirectory()) {
          transferDirectoryRecursive(file, file.getName(), outputStream, inputStream);
        }
      }
    }

    // End directory
    outputStream.write("E\n".getBytes());
    outputStream.flush();
    checkAck(inputStream);
  }

  /** Transfer single file in directory */
  private void transferFileInDirectory(
      final File localFile,
      final String remoteFileName,
      final java.io.OutputStream outputStream,
      final InputStream inputStream)
      throws Exception {
    FileInputStream fileInputStream = null;
    try {
      final long fileSize = localFile.length();
      final String fileCommand = String.format("C0644 %d %s\n", fileSize, remoteFileName);
      outputStream.write(fileCommand.getBytes());
      outputStream.flush();
      checkAck(inputStream);

      fileInputStream = new FileInputStream(localFile);
      final byte[] buffer = new byte[1024];
      long totalBytes = 0;
      while (totalBytes < fileSize) {
        final int bytesRead =
            fileInputStream.read(buffer, 0, (int) Math.min(buffer.length, fileSize - totalBytes));
        if (bytesRead < 0) {
          break;
        }
        outputStream.write(buffer, 0, bytesRead);
        totalBytes += bytesRead;
      }

      outputStream.write(0);
      outputStream.flush();
      checkAck(inputStream);
    } finally {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (final IOException e) {
          LOGGER.warn("Failed to close file input stream", e);
        }
      }
    }
  }

  /** Check SCP command response */
  private void checkAck(final InputStream inputStream) throws IOException {
    final int b = inputStream.read();
    if (b == 0) {
      return; // Success
    }
    if (b == -1) {
      throw new IOException("Unexpected end of stream");
    }
    if (b == 1 || b == 2) {
      // Error or warning
      final StringBuilder errorMsg = new StringBuilder();
      int c;
      while ((c = inputStream.read()) != '\n') {
        errorMsg.append((char) c);
      }
      if (b == 1) {
        throw new IOException("SCP error: " + errorMsg.toString());
      } else {
        LOGGER.warn("SCP warning: {}", errorMsg.toString());
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (session != null && session.isConnected()) {
      session.disconnect();
      LOGGER.debug("SSH session disconnected");
    }
  }
}
