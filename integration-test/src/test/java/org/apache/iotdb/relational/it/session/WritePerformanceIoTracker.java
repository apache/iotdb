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

package org.apache.iotdb.relational.it.session;

import org.apache.iotdb.isession.ITableSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Linux-only, opt-in counters sampled outside the single-row benchmark's timed write loops. */
final class WritePerformanceIoTracker {
  private static final Pattern TCP_COUNTER =
      Pattern.compile("\\b(bytes_received|bytes_sent|bytes_retrans):(\\d+)\\b");

  private final long dataNodePid;
  private final long configNodePid;
  private final int rpcPort;

  WritePerformanceIoTracker(long dataNodePid, long configNodePid, int rpcPort) throws IOException {
    assertTrue("I/O measurement requires Linux /proc", Files.isDirectory(Path.of("/proc")));
    assertTrue("DataNode PID unavailable", dataNodePid > 0);
    assertTrue("ConfigNode PID unavailable", configNodePid > 0);
    this.dataNodePid = dataNodePid;
    this.configNodePid = configNodePid;
    this.rpcPort = rpcPort;
    // Check readability early; unsupported counters must fail rather than appear as zero I/O.
    readDisk(dataNodePid);
    readDisk(configNodePid);
    System.out.printf(
        "io configuration: dataNodePid=%d, configNodePid=%d, rpcPort=%d, "
            + "network=server TCP payload counters, disk=/proc/pid/io, "
            + "flushBeforeAndAfter=true, compaction=false%n",
        dataNodePid, configNodePid, rpcPort);
  }

  Snapshot beforeRound(ITableSession session) throws Exception {
    // Drain previous MemTables/WAL before the baseline; the following flush is charged to this
    // round.
    session.executeNonQueryStatement("FLUSH");
    return new Snapshot(readNetwork(), readDisk(dataNodePid), readDisk(configNodePid));
  }

  Result afterRound(Snapshot before, ITableSession session) throws Exception {
    // Snapshot TCP before FLUSH so SQL/control traffic is excluded from write-request bytes.
    Map<String, Traffic> network = readNetwork();
    assertEquals(
        "RPC connections changed during I/O measurement",
        before.network.keySet(),
        network.keySet());
    Traffic traffic = new Traffic(0, 0, 0);
    for (Map.Entry<String, Traffic> entry : network.entrySet()) {
      traffic = traffic.plus(entry.getValue().minus(before.network.get(entry.getKey())));
    }
    Disk activeDataNode = readDisk(dataNodePid).minus(before.dataNode);
    long flushStart = System.nanoTime();
    session.executeNonQueryStatement("FLUSH");
    long flushNanos = System.nanoTime() - flushStart;
    Disk dataNode = readDisk(dataNodePid).minus(before.dataNode);
    Disk configNode = readDisk(configNodePid).minus(before.configNode);
    return new Result(traffic, activeDataNode, dataNode, configNode, flushNanos);
  }

  private Map<String, Traffic> readNetwork() throws Exception {
    // Count only server endpoints on this test DataNode's RPC port. Loopback RX/TX are not doubled.
    Process process =
        new ProcessBuilder("ss", "-tinHO", "state", "established", "( sport = :" + rpcPort + " )")
            .redirectErrorStream(true)
            .start();
    CompletableFuture<String> output =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
              } catch (IOException e) {
                throw new CompletionException(e);
              }
            });
    try {
      assertTrue("ss did not finish", process.waitFor(10, TimeUnit.SECONDS));
      String text = output.get(10, TimeUnit.SECONDS);
      assertEquals(text, 0, process.exitValue());
      Map<String, Traffic> result = new HashMap<>();
      for (String line : text.split("\\R")) {
        if (line.isBlank()) continue;
        String[] fields = line.trim().split("\\s+", 5);
        assertTrue("Unexpected ss output: " + line, fields.length == 5);
        Map<String, Long> counters = new HashMap<>();
        Matcher matcher = TCP_COUNTER.matcher(fields[4]);
        while (matcher.find()) counters.put(matcher.group(1), Long.parseLong(matcher.group(2)));
        assertTrue(
            "TCP byte counters unavailable: " + line,
            counters.containsKey("bytes_received") && counters.containsKey("bytes_sent"));
        String connection = fields[2] + "->" + fields[3];
        assertFalse("Duplicate server socket: " + connection, result.containsKey(connection));
        // ss omits zero-valued bytes_retrans; the received/sent counters above are mandatory.
        result.put(
            connection,
            new Traffic(
                counters.get("bytes_received"),
                counters.get("bytes_sent"),
                counters.getOrDefault("bytes_retrans", 0L)));
      }
      assertFalse("No established test RPC connections", result.isEmpty());
      return result;
    } finally {
      process.destroyForcibly();
      output.cancel(true);
    }
  }

  private static Disk readDisk(long pid) throws IOException {
    Map<String, Long> counters = new HashMap<>();
    for (String line : Files.readAllLines(Path.of("/proc", Long.toString(pid), "io"))) {
      String[] fields = line.split(":\\s*", 2);
      counters.put(fields[0], Long.parseLong(fields[1].trim()));
    }
    assertTrue(
        "Process storage byte counters unavailable for PID " + pid,
        counters.containsKey("read_bytes")
            && counters.containsKey("write_bytes")
            && counters.containsKey("cancelled_write_bytes"));
    // These are kernel-accounted process storage bytes, not rchar/wchar (which include socket I/O),
    // nor physical NAND bytes. Preserve cancelled writes separately instead of hiding them.
    return new Disk(
        counters.get("read_bytes"),
        counters.get("write_bytes"),
        counters.get("cancelled_write_bytes"));
  }

  record Snapshot(Map<String, Traffic> network, Disk dataNode, Disk configNode) {}

  private record Traffic(long received, long sent, long retransmitted) {
    Traffic minus(Traffic previous) {
      assertTrue(
          "TCP counters decreased",
          received >= previous.received
              && sent >= previous.sent
              && retransmitted >= previous.retransmitted);
      return new Traffic(
          received - previous.received,
          sent - previous.sent,
          retransmitted - previous.retransmitted);
    }

    Traffic plus(Traffic other) {
      return new Traffic(
          received + other.received, sent + other.sent, retransmitted + other.retransmitted);
    }
  }

  private record Disk(long read, long written, long cancelled) {
    Disk minus(Disk previous) {
      assertTrue(
          "Process I/O counters decreased",
          read >= previous.read && written >= previous.written && cancelled >= previous.cancelled);
      return new Disk(
          read - previous.read, written - previous.written, cancelled - previous.cancelled);
    }
  }

  record Result(
      Traffic traffic, Disk activeDataNode, Disk dataNode, Disk configNode, long flushNanos) {
    @Override
    public String toString() {
      return String.format(
          "requestBytes=%d, responseBytes=%d, responseRetransmitBytes=%d, "
              + "dnActiveReadBytes=%d, dnActiveWriteBytes=%d, dnActiveCancelledBytes=%d, "
              + "dnReadBytes=%d, dnWriteBytes=%d, dnCancelledBytes=%d, "
              + "cnReadBytes=%d, cnWriteBytes=%d, cnCancelledBytes=%d, flushNanos=%d",
          traffic.received,
          traffic.sent,
          traffic.retransmitted,
          activeDataNode.read,
          activeDataNode.written,
          activeDataNode.cancelled,
          dataNode.read,
          dataNode.written,
          dataNode.cancelled,
          configNode.read,
          configNode.written,
          configNode.cancelled,
          flushNanos);
    }
  }
}
