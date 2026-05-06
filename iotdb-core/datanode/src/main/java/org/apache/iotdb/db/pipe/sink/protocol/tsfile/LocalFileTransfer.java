package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
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
  public void transferFile(File tsFile, File modFile, File objectSourceDir, String targetName)
      throws IOException {
    if (tsFile == null || !tsFile.exists()) {
      throw new PipeException("Source TsFile is missing");
    }

    if (objectSourceDir != null && objectSourceDir.exists() && objectSourceDir.isDirectory()) {
      copyObjectDirectory(objectSourceDir, targetName);
    }

    final String finalTsName = finalTsFileName(targetName);
    exportModIfPresent(modFile, finalTsName);
    exportTsFile(tsFile, finalTsName);
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

  private static String finalTsFileName(final String targetName) {
    return targetName.endsWith(TsFileConstant.TSFILE_SUFFIX)
        ? targetName
        : targetName + TsFileConstant.TSFILE_SUFFIX;
  }

  private void exportTsFile(final File tsFile, final String finalTsName) throws IOException {
    final File dest = new File(targetBaseDir, finalTsName);
    transferByHardLinkOrCopy(tsFile, dest, "TsFile");
  }

  private void exportModIfPresent(final File modFile, final String finalTsName) throws IOException {
    if (modFile == null || !modFile.exists()) {
      return;
    }
    final String modDestName = finalTsName + ModificationFile.FILE_SUFFIX;
    final File modDest = new File(targetBaseDir, modDestName);
    transferByHardLinkOrCopy(modFile, modDest, "TsFile mod");
  }

  void transferByHardLinkOrCopy(final File source, final File dest, final String fileType)
      throws IOException {
    if (source.getCanonicalFile().equals(dest.getCanonicalFile())) {
      LOGGER.info(
          "Skip exporting {} because source and target are the same file: {}",
          fileType,
          dest.getAbsolutePath());
      return;
    }

    final Path targetDirectory = dest.getParentFile().toPath();
    if (!Files.exists(targetDirectory)) {
      Files.createDirectories(targetDirectory);
    }

    if (isSameFileStore(source.toPath(), targetDirectory)) {
      try {
        createHardLink(source, dest);
        LOGGER.info("Successfully hard-linked {} to {}", fileType, dest.getAbsolutePath());
        return;
      } catch (final IOException e) {
        LOGGER.warn(
            "Failed to hard-link {} to {}, fallback to copy.", fileType, dest.getAbsolutePath(), e);
      }
    } else {
      LOGGER.info(
          "Detected cross-filesystem export for {}, fallback to copy: {}",
          fileType,
          dest.getAbsolutePath());
    }

    copyFile(source, dest);
    LOGGER.info("Successfully copied {} to {}", fileType, dest.getAbsolutePath());
  }

  boolean isSameFileStore(final Path sourcePath, final Path targetDirectory) throws IOException {
    return Files.getFileStore(sourcePath).equals(Files.getFileStore(targetDirectory));
  }

  void createHardLink(final File source, final File dest) throws IOException {
    FileUtils.createHardLink(source, dest);
  }

  private void copyFile(final File source, final File dest) throws IOException {
    Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public void close() {
    // No-op because local transfer does not keep any open connection or file handle.
  }
}
