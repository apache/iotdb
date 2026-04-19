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

package org.apache.iotdb;

import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Inspect a single WAL file and print size breakdowns for its major sections.
 *
 * <p>Example:
 *
 * <pre>
 *   java ... org.apache.iotdb.ConsensusSubscriptionWalFileAnalyzer D:\path\to\_12-25000-1.wal
 * </pre>
 */
public class ConsensusSubscriptionWalFileAnalyzer {

  private static final String V1_MAGIC = "WAL";
  private static final String V2_MAGIC = "V2-WAL";
  private static final String V3_MAGIC = "V3-WAL";

  private static final int SEGMENT_HEADER_BASE_BYTES = Byte.BYTES + Integer.BYTES;
  private static final int COMPRESSED_SEGMENT_EXTRA_HEADER_BYTES = Integer.BYTES;
  private static final int WAL_FILE_INFO_END_MARKER_BYTES = Byte.BYTES;
  private static final int METADATA_SIZE_FIELD_BYTES = Integer.BYTES;
  private static final int V3_EMPTY_METADATA_REMAINING_WITHOUT_MEMTABLE_COUNT =
      Long.BYTES * 2 + Short.BYTES * 2 + Integer.BYTES;

  public static void main(final String[] args) throws Exception {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      printUsage();
      return;
    }

    final File walFile = new File(args[0]);
    if (!walFile.isFile()) {
      throw new IllegalArgumentException("WAL file does not exist: " + walFile.getAbsolutePath());
    }

    final WalFileAnalysis analysis = analyze(walFile);
    printAnalysis(analysis);
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println(
        "  java ... org.apache.iotdb.ConsensusSubscriptionWalFileAnalyzer <wal-file-path>");
  }

  private static WalFileAnalysis analyze(final File walFile) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(walFile, "r");
        FileChannel channel = raf.getChannel()) {
      final long totalBytes = channel.size();
      final String version = detectVersion(channel, totalBytes);
      final int headMagicBytes = getHeadMagicBytes(version);
      final int tailMagicBytes = getTailMagicBytes(version);

      final WalFileAnalysis analysis = new WalFileAnalysis(walFile, version, totalBytes);
      analysis.headMagicBytes = Math.min(totalBytes, headMagicBytes);

      if (totalBytes <= headMagicBytes) {
        analysis.note = "header-only WAL file (magic only, no body/footer)";
        return analysis;
      }

      if (!hasTrailingMagic(channel, totalBytes, version)) {
        analysis.note = "missing trailing magic/footer, file may be open or broken";
        return analysis;
      }

      analysis.tailMagicBytes = tailMagicBytes;
      analysis.metadataSizeFieldBytes = METADATA_SIZE_FIELD_BYTES;

      final long metadataSizeFieldPos = totalBytes - tailMagicBytes - METADATA_SIZE_FIELD_BYTES;
      if (metadataSizeFieldPos < headMagicBytes) {
        analysis.note = "invalid metadata size position";
        return analysis;
      }

      final int metadataBytes = readInt(channel, metadataSizeFieldPos);
      analysis.metadataBytes = metadataBytes;
      analysis.footerStartOffset = metadataSizeFieldPos - metadataBytes;
      if (analysis.footerStartOffset < headMagicBytes) {
        analysis.note = "invalid footer start offset";
        return analysis;
      }

      final long markerOffset = analysis.footerStartOffset - WAL_FILE_INFO_END_MARKER_BYTES;
      if (markerOffset < headMagicBytes) {
        analysis.note = "invalid end-marker offset";
        return analysis;
      }

      analysis.endMarkerBytes = WAL_FILE_INFO_END_MARKER_BYTES;
      analysis.segmentStartOffset = headMagicBytes;
      analysis.segmentEndOffsetExclusive = markerOffset;
      analysis.segmentRegionBytes = Math.max(0L, markerOffset - headMagicBytes);

      scanSegments(channel, analysis);
      parseFooter(channel, analysis);
      return analysis;
    }
  }

  private static void scanSegments(final FileChannel channel, final WalFileAnalysis analysis)
      throws IOException {
    long offset = analysis.segmentStartOffset;
    while (offset < analysis.segmentEndOffsetExclusive) {
      if (analysis.segmentEndOffsetExclusive - offset < SEGMENT_HEADER_BASE_BYTES) {
        analysis.segmentParseWarning =
            "remaining bytes are smaller than a segment header at offset " + offset;
        return;
      }

      final ByteBuffer headerBuffer = ByteBuffer.allocate(SEGMENT_HEADER_BASE_BYTES);
      readFully(channel, headerBuffer, offset);
      headerBuffer.flip();

      final CompressionType compressionType = CompressionType.deserialize(headerBuffer.get());
      final int dataInDiskBytes = headerBuffer.getInt();
      int headerBytes = SEGMENT_HEADER_BASE_BYTES;
      if (compressionType != CompressionType.UNCOMPRESSED) {
        headerBytes += COMPRESSED_SEGMENT_EXTRA_HEADER_BYTES;
      }

      final long nextOffset = offset + headerBytes + dataInDiskBytes;
      if (nextOffset > analysis.segmentEndOffsetExclusive) {
        analysis.segmentParseWarning =
            String.format(
                Locale.ROOT,
                "segment at offset %d exceeds body boundary (%d > %d)",
                offset,
                nextOffset,
                analysis.segmentEndOffsetExclusive);
        return;
      }

      analysis.segmentCount++;
      analysis.segmentHeaderBytes += headerBytes;
      analysis.segmentPayloadBytes += dataInDiskBytes;
      if (compressionType != CompressionType.UNCOMPRESSED) {
        analysis.compressedSegmentCount++;
      }
      offset = nextOffset;
    }

    if (offset != analysis.segmentEndOffsetExclusive) {
      analysis.segmentParseWarning =
          String.format(
              Locale.ROOT,
              "segment parser stopped at %d but expected %d",
              offset,
              analysis.segmentEndOffsetExclusive);
    }
  }

  private static void parseFooter(final FileChannel channel, final WalFileAnalysis analysis)
      throws IOException {
    if (analysis.metadataBytes <= 0) {
      return;
    }

    final ByteBuffer metadataBuffer = ByteBuffer.allocate(analysis.metadataBytes);
    readFully(channel, metadataBuffer, analysis.footerStartOffset);
    metadataBuffer.flip();

    if (metadataBuffer.remaining() < Long.BYTES + Integer.BYTES) {
      analysis.footerWarning = "metadata buffer is too small";
      return;
    }

    metadataBuffer.getLong();
    analysis.firstSearchIndexBytes = Long.BYTES;
    final int entryCount = metadataBuffer.getInt();
    analysis.entryCount = entryCount;
    analysis.entryCountBytes = Integer.BYTES;

    analysis.bufferSizeArrayBytes = (long) entryCount * Integer.BYTES;
    for (int i = 0; i < entryCount; i++) {
      metadataBuffer.getInt();
    }

    final boolean serializedEmptyV3WithoutMemTableCount =
        V3_MAGIC.equals(analysis.version)
            && entryCount == 0
            && metadataBuffer.remaining() == V3_EMPTY_METADATA_REMAINING_WITHOUT_MEMTABLE_COUNT;

    if (metadataBuffer.hasRemaining() && !serializedEmptyV3WithoutMemTableCount) {
      analysis.memTableCountFieldBytes = Integer.BYTES;
      analysis.memTableCount = metadataBuffer.getInt();
      analysis.memTableIdsBytes = (long) analysis.memTableCount * Long.BYTES;
      for (int i = 0; i < analysis.memTableCount; i++) {
        metadataBuffer.getLong();
      }
    }

    if (V3_MAGIC.equals(analysis.version) && metadataBuffer.hasRemaining()) {
      if (metadataBuffer.remaining() < Long.BYTES * 2) {
        analysis.footerWarning = "V3 metadata is truncated before min/max timestamp range";
        return;
      }

      analysis.minMaxDataTsBytes = Long.BYTES * 2L;
      metadataBuffer.getLong();
      metadataBuffer.getLong();

      final long requiredWriterMetadataBytes =
          (long) entryCount * Long.BYTES * 2 + Short.BYTES * 2 + Integer.BYTES;
      if (metadataBuffer.remaining() < requiredWriterMetadataBytes) {
        analysis.footerWarning = "V3 metadata is truncated before writer progress arrays";
        return;
      }

      analysis.physicalTimesBytes = (long) entryCount * Long.BYTES;
      analysis.localSeqsBytes = (long) entryCount * Long.BYTES;
      for (int i = 0; i < entryCount; i++) {
        metadataBuffer.getLong();
      }
      for (int i = 0; i < entryCount; i++) {
        metadataBuffer.getLong();
      }

      analysis.defaultWriterIdentityBytes = Short.BYTES * 2L;
      metadataBuffer.getShort();
      metadataBuffer.getShort();

      analysis.overrideCountFieldBytes = Integer.BYTES;
      analysis.overrideCount = metadataBuffer.getInt();

      analysis.overrideIndexesBytes = (long) analysis.overrideCount * Integer.BYTES;
      analysis.overrideNodeIdsBytes = (long) analysis.overrideCount * Short.BYTES;
      analysis.overrideWriterEpochsBytes = (long) analysis.overrideCount * Short.BYTES;

      for (int i = 0; i < analysis.overrideCount; i++) {
        metadataBuffer.getInt();
      }
      for (int i = 0; i < analysis.overrideCount; i++) {
        metadataBuffer.getShort();
      }
      for (int i = 0; i < analysis.overrideCount; i++) {
        metadataBuffer.getShort();
      }
    }

    analysis.unknownMetadataBytes = metadataBuffer.remaining();
  }

  private static String detectVersion(final FileChannel channel, final long totalBytes)
      throws IOException {
    if (totalBytes >= V3_MAGIC.length()
        && readString(channel, 0, V3_MAGIC.length()).equals(V3_MAGIC)) {
      return V3_MAGIC;
    }
    if (totalBytes >= V2_MAGIC.length()
        && readString(channel, 0, V2_MAGIC.length()).equals(V2_MAGIC)) {
      return V2_MAGIC;
    }
    if (totalBytes >= V1_MAGIC.length()
        && readString(channel, totalBytes - V1_MAGIC.length(), V1_MAGIC.length())
            .equals(V1_MAGIC)) {
      return V1_MAGIC;
    }
    return "UNKNOWN";
  }

  private static int getHeadMagicBytes(final String version) {
    if (V3_MAGIC.equals(version)) {
      return V3_MAGIC.length();
    }
    if (V2_MAGIC.equals(version)) {
      return V2_MAGIC.length();
    }
    return 0;
  }

  private static int getTailMagicBytes(final String version) {
    if (V3_MAGIC.equals(version)) {
      return V3_MAGIC.length();
    }
    if (V2_MAGIC.equals(version)) {
      return V2_MAGIC.length();
    }
    if (V1_MAGIC.equals(version)) {
      return V1_MAGIC.length();
    }
    return 0;
  }

  private static boolean hasTrailingMagic(
      final FileChannel channel, final long totalBytes, final String version) throws IOException {
    final int tailMagicBytes = getTailMagicBytes(version);
    if (tailMagicBytes <= 0 || totalBytes < tailMagicBytes) {
      return false;
    }
    return readString(channel, totalBytes - tailMagicBytes, tailMagicBytes).equals(version);
  }

  private static String readString(final FileChannel channel, final long offset, final int length)
      throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    readFully(channel, buffer, offset);
    buffer.flip();
    return StandardCharsets.UTF_8.decode(buffer).toString();
  }

  private static int readInt(final FileChannel channel, final long offset) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    readFully(channel, buffer, offset);
    buffer.flip();
    return buffer.getInt();
  }

  private static void readFully(
      final FileChannel channel, final ByteBuffer buffer, final long offset) throws IOException {
    long position = offset;
    while (buffer.hasRemaining()) {
      final int bytesRead = channel.read(buffer, position);
      if (bytesRead < 0) {
        throw new IOException("Unexpected EOF while reading at offset " + position);
      }
      position += bytesRead;
    }
  }

  private static void printAnalysis(final WalFileAnalysis analysis) {
    System.out.println("=== WAL File Layout Analysis ===");
    System.out.println("file: " + analysis.file.getAbsolutePath());
    System.out.println("version: " + analysis.version);
    System.out.println("total: " + formatBytes(analysis.totalBytes));
    if (analysis.note != null) {
      System.out.println("note: " + analysis.note);
    }
    System.out.println();

    printSection("head magic", analysis.headMagicBytes, analysis.totalBytes);
    printSection("segment headers", analysis.segmentHeaderBytes, analysis.totalBytes);
    printSection("segment payload", analysis.segmentPayloadBytes, analysis.totalBytes);
    printSection("wal end marker", analysis.endMarkerBytes, analysis.totalBytes);
    printSection("footer metadata", analysis.metadataBytes, analysis.totalBytes);
    printSection("metadata size field", analysis.metadataSizeFieldBytes, analysis.totalBytes);
    printSection("tail magic", analysis.tailMagicBytes, analysis.totalBytes);
    final long accountedBytes =
        analysis.headMagicBytes
            + analysis.segmentHeaderBytes
            + analysis.segmentPayloadBytes
            + analysis.endMarkerBytes
            + analysis.metadataBytes
            + analysis.metadataSizeFieldBytes
            + analysis.tailMagicBytes;
    if (analysis.totalBytes >= accountedBytes) {
      printSection("unaccounted", analysis.totalBytes - accountedBytes, analysis.totalBytes);
    }

    System.out.println();
    System.out.println(
        String.format(
            Locale.ROOT,
            "segments: total=%d, compressed=%d",
            analysis.segmentCount,
            analysis.compressedSegmentCount));
    if (analysis.segmentParseWarning != null) {
      System.out.println("segment warning: " + analysis.segmentParseWarning);
    }

    if (analysis.metadataBytes <= 0) {
      return;
    }

    System.out.println();
    System.out.println("=== Footer Breakdown ===");
    printSection("v2-compatible base", analysis.getV2BaseMetadataBytes(), analysis.totalBytes);
    if (V3_MAGIC.equals(analysis.version)) {
      printSection("v3 extension total", analysis.getV3ExtensionBytes(), analysis.totalBytes);
      System.out.println(
          String.format(
              Locale.ROOT,
              "v3 extension share of footer: %s",
              formatPercent(analysis.getV3ExtensionBytes(), analysis.metadataBytes)));
      printSection("  min/max data ts", analysis.minMaxDataTsBytes, analysis.totalBytes);
      printSection("  physicalTimes[]", analysis.physicalTimesBytes, analysis.totalBytes);
      printSection("  localSeqs[]", analysis.localSeqsBytes, analysis.totalBytes);
      printSection(
          "  default writer identity + override count",
          analysis.defaultWriterIdentityBytes + analysis.overrideCountFieldBytes,
          analysis.totalBytes);
      printSection("  overrideIndexes[]", analysis.overrideIndexesBytes, analysis.totalBytes);
      printSection("  overrideNodeIds[]", analysis.overrideNodeIdsBytes, analysis.totalBytes);
      printSection(
          "  overrideWriterEpochs[]", analysis.overrideWriterEpochsBytes, analysis.totalBytes);
    }
    if (analysis.unknownMetadataBytes > 0) {
      printSection("unknown metadata tail", analysis.unknownMetadataBytes, analysis.totalBytes);
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "entries=%d, memTables=%d, overrides=%d",
            analysis.entryCount,
            analysis.memTableCount,
            analysis.overrideCount));
    if (analysis.footerWarning != null) {
      System.out.println("footer warning: " + analysis.footerWarning);
    }
  }

  private static void printSection(final String name, final long bytes, final long totalBytes) {
    System.out.println(
        String.format(
            Locale.ROOT,
            "%-42s %12s  %8s",
            name + ":",
            formatBytes(bytes),
            formatPercent(bytes, totalBytes)));
  }

  private static String formatBytes(final long bytes) {
    final long absBytes = Math.abs(bytes);
    if (absBytes < 1024L) {
      return bytes + " B";
    }
    if (absBytes < 1024L * 1024L) {
      return String.format(Locale.ROOT, "%.2f KiB", bytes / 1024.0d);
    }
    if (absBytes < 1024L * 1024L * 1024L) {
      return String.format(Locale.ROOT, "%.2f MiB", bytes / 1024.0d / 1024.0d);
    }
    return String.format(Locale.ROOT, "%.2f GiB", bytes / 1024.0d / 1024.0d / 1024.0d);
  }

  private static String formatPercent(final long bytes, final long totalBytes) {
    if (totalBytes <= 0) {
      return "N/A";
    }
    return String.format(Locale.ROOT, "%.2f%%", bytes * 100.0d / totalBytes);
  }

  private static final class WalFileAnalysis {
    private final File file;
    private final String version;
    private final long totalBytes;

    private long headMagicBytes;
    private long segmentHeaderBytes;
    private long segmentPayloadBytes;
    private long endMarkerBytes;
    private int metadataBytes;
    private long metadataSizeFieldBytes;
    private long tailMagicBytes;

    private long footerStartOffset;
    private long segmentStartOffset;
    private long segmentEndOffsetExclusive;
    private long segmentRegionBytes;

    private int segmentCount;
    private int compressedSegmentCount;

    private int entryCount;
    private int memTableCount;
    private int overrideCount;
    private long firstSearchIndexBytes;
    private long entryCountBytes;
    private long bufferSizeArrayBytes;
    private long memTableCountFieldBytes;
    private long memTableIdsBytes;
    private long minMaxDataTsBytes;
    private long physicalTimesBytes;
    private long localSeqsBytes;
    private long defaultWriterIdentityBytes;
    private long overrideCountFieldBytes;
    private long overrideIndexesBytes;
    private long overrideNodeIdsBytes;
    private long overrideWriterEpochsBytes;
    private long unknownMetadataBytes;

    private String note;
    private String segmentParseWarning;
    private String footerWarning;

    private WalFileAnalysis(final File file, final String version, final long totalBytes) {
      this.file = file;
      this.version = version;
      this.totalBytes = totalBytes;
    }

    private long getV2BaseMetadataBytes() {
      return firstSearchIndexBytes
          + entryCountBytes
          + bufferSizeArrayBytes
          + memTableCountFieldBytes
          + memTableIdsBytes;
    }

    private long getV3ExtensionBytes() {
      return minMaxDataTsBytes
          + physicalTimesBytes
          + localSeqsBytes
          + defaultWriterIdentityBytes
          + overrideCountFieldBytes
          + overrideIndexesBytes
          + overrideNodeIdsBytes
          + overrideWriterEpochsBytes;
    }
  }
}
