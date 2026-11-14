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

package org.apache.iotdb.consensus.raft.snapshot;

import org.apache.iotdb.consensus.raft.storage.AtomicFiles;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Portable checkpoint inventory and optional ZIP transport. No file outside the checkpoint is sent.
 */
public final class SnapshotBundle {
  public static final String INVENTORY = "files.manifest";
  public static final String ARCHIVE = "checkpoint.zip";
  private static final int MAGIC = 0x4c534d31;
  private static final int MAX_FILES = 1000000;

  private SnapshotBundle() {}

  public static final class Item {
    public final String path;
    public final long length;
    public final byte[] hash;

    Item(String path, long length, byte[] hash) {
      this.path = path;
      this.length = length;
      this.hash = hash;
    }
  }

  /** Freeze the complete inventory after the state machine has captured its applied boundary. */
  public static void create(File root, List<File> files) throws IOException {
    if (files == null) {
      throw new IOException("state machine returned no snapshot file list");
    }
    List<Item> items = new ArrayList<>();
    Set<String> names = new HashSet<>();
    for (File file : files) {
      Path path = file.toPath().toRealPath();
      Path parent = root.toPath().toRealPath();
      if (!path.startsWith(parent) || !Files.isRegularFile(path)) {
        throw new IOException("snapshot file must be inside its portable root: " + file);
      }
      String rel = parent.relativize(path).toString().replace(File.separatorChar, '/');
      if (rel.equals(INVENTORY) || rel.equals(ARCHIVE)) {
        continue;
      }
      if (!names.add(rel)) {
        continue;
      }
      items.add(new Item(rel, Files.size(path), digest(path)));
      force(path);
    }
    items.sort((a, b) -> a.path.compareTo(b.path));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(MAGIC);
      out.writeInt(items.size());
      for (Item item : items) {
        out.writeUTF(item.path);
        out.writeLong(item.length);
        out.write(item.hash);
      }
    }
    AtomicFiles.write(new File(root, INVENTORY), bytes.toByteArray());
    forceDirectories(root);
  }

  public static List<Item> inventory(File root) throws IOException {
    byte[] bytes = AtomicFiles.read(new File(root, INVENTORY));
    if (bytes == null) {
      throw new IOException("missing or corrupt checkpoint inventory");
    }
    List<Item> items = new ArrayList<>();
    Set<String> names = new HashSet<>();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      if (in.readInt() != MAGIC) {
        throw new IOException("unsupported checkpoint inventory");
      }
      int count = in.readInt();
      if (count < 0 || count > MAX_FILES) {
        throw new IOException("invalid checkpoint file count");
      }
      for (int i = 0; i < count; i++) {
        String name = in.readUTF();
        resolve(root, name);
        long length = in.readLong();
        if (length < 0 || !names.add(name) || name.equals(INVENTORY) || name.equals(ARCHIVE)) {
          throw new IOException("invalid checkpoint inventory entry: " + name);
        }
        byte[] hash = new byte[32];
        in.readFully(hash);
        items.add(new Item(name, length, hash));
      }
      if (in.read() != -1) {
        throw new IOException("trailing checkpoint inventory data");
      }
    }
    return items;
  }

  public static void verify(File root) throws IOException {
    Set<String> expected = new HashSet<>();
    expected.add(INVENTORY);
    expected.add(ARCHIVE);
    expected.add(ARCHIVE + ".tmp"); // a concurrently generated, never installed transport cache
    for (Item item : inventory(root)) {
      expected.add(item.path);
      Path file = resolve(root, item.path);
      if (!Files.isRegularFile(file)
          || Files.size(file) != item.length
          || !MessageDigest.isEqual(item.hash, digest(file))) {
        throw new IOException("incomplete or corrupt checkpoint file: " + item.path);
      }
    }
    try (java.util.stream.Stream<Path> paths = Files.walk(root.toPath())) {
      java.util.Iterator<Path> files = paths.filter(Files::isRegularFile).iterator();
      while (files.hasNext()) {
        String relative =
            root.toPath().relativize(files.next()).toString().replace(File.separatorChar, '/');
        if (!expected.contains(relative)) {
          throw new IOException("unexpected checkpoint file: " + relative);
        }
      }
    }
  }

  public static long rawBytes(File root) throws IOException {
    long total = 0;
    for (Item item : inventory(root)) {
      total = Math.addExact(total, item.length);
    }
    return total;
  }

  public static File archive(File root) throws IOException {
    File archive = new File(root, ARCHIVE);
    if (archive.isFile()) {
      return archive;
    }
    File tmp = new File(root, ARCHIVE + ".tmp");
    try (ZipOutputStream zip =
        new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp.toPath())))) {
      add(zip, root, INVENTORY);
      for (Item item : inventory(root)) {
        add(zip, root, item.path);
      }
    }
    force(tmp.toPath());
    Files.move(tmp.toPath(), archive.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    return archive;
  }

  private static void add(ZipOutputStream zip, File root, String name) throws IOException {
    zip.putNextEntry(new ZipEntry(name));
    Files.copy(resolve(root, name), zip);
    zip.closeEntry();
  }

  /** Inventory is the first ZIP entry. Limit every subsequent entry to its declared size. */
  public static void unpack(File root) throws IOException {
    Set<String> seen = new HashSet<>();
    java.util.Map<String, Item> expected = new java.util.HashMap<>();
    try (ZipInputStream zip =
        new ZipInputStream(
            new BufferedInputStream(Files.newInputStream(new File(root, ARCHIVE).toPath())))) {
      ZipEntry entry = zip.getNextEntry();
      if (entry == null || !INVENTORY.equals(entry.getName())) {
        throw new IOException("checkpoint ZIP must begin with its inventory");
      }
      copyLimited(zip, resolve(root, INVENTORY), 64L << 20);
      zip.closeEntry();
      for (Item item : inventory(root)) {
        expected.put(item.path, item);
      }
      while ((entry = zip.getNextEntry()) != null) {
        Item item = expected.get(entry.getName());
        if (item == null || !seen.add(item.path) || entry.isDirectory()) {
          throw new IOException("unexpected checkpoint ZIP entry: " + entry.getName());
        }
        copyLimited(zip, resolve(root, item.path), item.length);
        zip.closeEntry();
      }
    }
    if (seen.size() != expected.size()) {
      throw new IOException("checkpoint ZIP is incomplete");
    }
    verify(root);
    Files.delete(new File(root, ARCHIVE).toPath());
  }

  private static void copyLimited(InputStream in, Path file, long limit) throws IOException {
    Files.createDirectories(file.getParent());
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
      byte[] buffer = new byte[65536];
      long count = 0;
      int n;
      while ((n = in.read(buffer)) != -1) {
        count += n;
        if (count > limit) {
          throw new IOException("checkpoint entry exceeds declared size");
        }
        out.write(buffer, 0, n);
      }
    }
    force(file);
  }

  public static Path resolve(File root, String relative) throws IOException {
    Path base = root.toPath().toAbsolutePath().normalize();
    Path supplied = java.nio.file.Paths.get(relative);
    Path path = base.resolve(supplied).normalize();
    if (relative.isEmpty()
        || supplied.isAbsolute()
        || !path.startsWith(base)
        || path.equals(base)
        || relative.contains("\\")
        || relative.indexOf('\0') >= 0) {
      throw new IOException("illegal checkpoint path: " + relative);
    }
    // The staging tree is created by us; reject symbolic links on every existing path component.
    Path p = path;
    while (!p.equals(base)) {
      if (Files.isSymbolicLink(p)) {
        throw new IOException("checkpoint path contains a symbolic link");
      }
      p = p.getParent();
    }
    return path;
  }

  private static byte[] digest(Path file) throws IOException {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    try (InputStream in = Files.newInputStream(file)) {
      byte[] buffer = new byte[65536];
      int n;
      while ((n = in.read(buffer)) != -1) {
        digest.update(buffer, 0, n);
      }
    }
    return digest.digest();
  }

  /** Persist directory entries as well as file contents on providers supporting directory fsync. */
  public static void forceDirectories(File root) throws IOException {
    List<Path> directories = new ArrayList<>();
    try (java.util.stream.Stream<Path> paths = Files.walk(root.toPath())) {
      paths.filter(Files::isDirectory).forEach(directories::add);
    }
    directories.sort((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount()));
    for (Path directory : directories) {
      forceDirectory(directory.toFile());
    }
  }

  public static void forceDirectory(File directory) throws IOException {
    // Windows providers do not expose a directory handle through FileChannel. File contents
    // still use force(true); Unix deployments also persist publication and journal directory
    // entries.
    if (System.getProperty("os.name", "").startsWith("Windows")) {
      return;
    }
    try (FileChannel channel = FileChannel.open(directory.toPath(), StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  static void force(Path file) throws IOException {
    try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
      ch.force(true);
    }
  }
}
