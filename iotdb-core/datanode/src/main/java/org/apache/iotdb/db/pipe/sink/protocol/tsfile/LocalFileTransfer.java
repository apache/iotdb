package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOCAL_TARGET_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LOCAL_TARGET_PATH_KEY;

public class LocalFileTransfer implements FileTransfer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileTransfer.class);
  private final String targetBaseDir;

  public LocalFileTransfer(PipeParameters parameters) {
    this.targetBaseDir =
        parameters.getStringByKeys(CONNECTOR_LOCAL_TARGET_PATH_KEY, SINK_LOCAL_TARGET_PATH_KEY);
    if (targetBaseDir == null || targetBaseDir.isEmpty()) {
      throw new IllegalArgumentException("Target path is required for local transfer");
    }
  }

  @Override
  public void handshake() throws IOException {
    final File dir = new File(targetBaseDir);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Failed to initialize target directory: " + targetBaseDir);
    }
  }

  @Override
  public void transferFile(File tsFile, File objectSourceDir, String targetName)
      throws IOException {
    if (tsFile == null || !tsFile.exists()) {
      throw new PipeException("Source TsFile is missing");
    }

    if (objectSourceDir != null && objectSourceDir.exists() && objectSourceDir.isDirectory()) {
      copyObjectDirectory(objectSourceDir, targetName);
    }

    copyTsFile(tsFile, targetName);
  }

  private void copyObjectDirectory(File sourceDir, String dirName) throws IOException {
    Path sourceRoot = sourceDir.toPath();
    Path targetRoot = new File(targetBaseDir, dirName).toPath();

    try (Stream<Path> stream = Files.walk(sourceRoot)) {
      stream.forEach(
          source -> {
            try {
              Path relativePath = sourceRoot.relativize(source);
              Path dest = targetRoot.resolve(relativePath);

              if (Files.isDirectory(source)) {
                if (!Files.exists(dest)) {
                  Files.createDirectories(dest);
                }
              } else {
                if (!Files.exists(dest.getParent())) {
                  Files.createDirectories(dest.getParent());
                }
                Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
              }
            } catch (IOException e) {
              throw new UncheckedIOException("Local object transfer failed", e);
            }
          });
    } catch (final UncheckedIOException e) {
      throw new IOException(e.getMessage(), e.getCause());
    }
  }

  private void copyTsFile(File tsFile, String targetName) throws IOException {
    String finalTsName = targetName.endsWith(".tsfile") ? targetName : targetName + ".tsfile";
    File dest = new File(targetBaseDir, finalTsName);

    Files.copy(tsFile.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    LOGGER.info("Successfully transferred TsFile to {}", dest.getAbsolutePath());
  }

  @Override
  public void close() {
    // No external resource is maintained by local transfer.
  }
}
