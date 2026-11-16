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

package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.Win32Exception;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinDef.DWORD;
import com.sun.jna.platform.win32.WinNT;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** CheckpointWriter writes the binary {@link Checkpoint} into .checkpoint file. */
public class CheckpointWriter extends LogWriter {
  private static final int FILE_RENAME_INFO = 3;

  private File currentFile;

  public CheckpointWriter(File logFile) throws IOException {
    super(logFile, WALFormats.writeVersion(WALFileVersion.V2));
    currentFile = logFile;
  }

  /** Durably renames a forced checkpoint in the same directory, keeping its writer open. */
  public void publish(File targetFile) throws IOException {
    Path source = currentFile.toPath();
    Path target = targetFile.toPath();
    // The manager owns the directory. Check for a stale target since ATOMIC_MOVE may replace it.
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      throw new FileAlreadyExistsException(target.toString());
    }
    if (Platform.isWindows()) {
      publishOnWindows(source, targetFile);
    } else {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
      currentFile = targetFile;
    }
    forceParentDirectory(target.toAbsolutePath().getParent());
  }

  protected void forceParentDirectory(Path directory) throws IOException {
    if (!Platform.isWindows()) {
      try (FileChannel channel = FileChannel.open(directory)) {
        channel.force(true);
      }
    }
  }

  private void publishOnWindows(Path source, File target) throws IOException {
    // Renaming through a write-through handle flushes the resulting NTFS metadata changes.
    Kernel32 kernel = Kernel32.INSTANCE;
    WinNT.HANDLE handle =
        kernel.CreateFile(
            windowsPath(source),
            WinNT.GENERIC_WRITE | WinNT.DELETE,
            WinNT.FILE_SHARE_READ | WinNT.FILE_SHARE_WRITE | WinNT.FILE_SHARE_DELETE,
            null,
            WinNT.OPEN_EXISTING,
            WinNT.FILE_FLAG_WRITE_THROUGH,
            null);
    if (WinBase.INVALID_HANDLE_VALUE.equals(handle)) {
      throw new IOException(new Win32Exception(Native.getLastError()));
    }

    try (Closeable closeHandle = () -> Kernel32Util.closeHandle(handle)) {
      String targetName = windowsPath(target.toPath());
      int nameBytes = targetName.length() * Native.WCHAR_SIZE;
      // FILE_RENAME_INFO has a zeroed flag and parent handle before the byte length and name.
      int lengthOffset = 2 * Native.POINTER_SIZE;
      int nameOffset = lengthOffset + Integer.BYTES;
      int bufferSize = nameOffset + nameBytes + Native.WCHAR_SIZE;
      try (Memory renameInfo = new Memory(bufferSize)) {
        renameInfo.clear();
        renameInfo.setInt(lengthOffset, nameBytes);
        renameInfo.setWideString(nameOffset, targetName);
        if (!kernel.SetFileInformationByHandle(
            handle, FILE_RENAME_INFO, renameInfo, new DWORD(bufferSize))) {
          IOException failure = new IOException(new Win32Exception(Native.getLastError()));
          if (!Files.exists(source, LinkOption.NOFOLLOW_LINKS)) {
            // The rename may have taken effect before the metadata flush failed.
            currentFile = target;
          }
          throw failure;
        }
        currentFile = target;
      }
    } catch (Win32Exception e) {
      throw new IOException(e);
    }
  }

  private static String windowsPath(Path path) {
    String absolutePath = path.toAbsolutePath().normalize().toString();
    return absolutePath.startsWith("\\\\")
        ? "\\\\?\\UNC\\" + absolutePath.substring(2)
        : "\\\\?\\" + absolutePath;
  }

  @Override
  public File getLogFile() {
    return currentFile;
  }
}
