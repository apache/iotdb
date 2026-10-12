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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.relational.it.session;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Samples the DataNode process resource usage while one benchmark round is running. */
final class WritePerformanceResourceTracker implements AutoCloseable {
  private static final Path PROC = Path.of("/proc");
  private static final long SAMPLE_INTERVAL_MILLIS = 100;

  private final long dataNodePid;
  private final long clockTicksPerSecond;
  private final Thread sampler;
  private final AtomicReference<Round> currentRound = new AtomicReference<>();
  private final AtomicReference<Throwable> samplingFailure = new AtomicReference<>();
  private final CountDownLatch stopped = new CountDownLatch(1);

  WritePerformanceResourceTracker(long dataNodePid) throws IOException {
    this.dataNodePid = dataNodePid;
    if (!isSupported()) {
      clockTicksPerSecond = 0;
      sampler = null;
      System.out.println("resource configuration: DataNode CPU/memory sampling unavailable");
      return;
    }
    if (dataNodePid <= 0) {
      throw new IOException("DataNode PID unavailable: " + dataNodePid);
    }
    clockTicksPerSecond = readClockTicksPerSecond();
    readProcessSample();
    sampler = new Thread(this::sampleLoop, "numeric-row-resource-sampler");
    sampler.setDaemon(true);
    sampler.start();
    System.out.printf(
        "resource configuration: dataNodePid=%d, sampleIntervalMs=%d, cpu=process CPU, "
            + "memory=resident set size%n",
        dataNodePid, SAMPLE_INTERVAL_MILLIS);
  }

  boolean isSupported() {
    return Files.isDirectory(PROC);
  }

  void beginRound() throws IOException {
    checkFailure();
    if (!isSupported()) {
      return;
    }
    Round round = new Round();
    round.sample();
    currentRound.set(round);
  }

  RoundPeak endRound() throws IOException {
    if (!isSupported()) {
      return RoundPeak.UNAVAILABLE;
    }
    Round round = currentRound.getAndSet(null);
    if (round == null) {
      throw new IllegalStateException("Resource round was not started");
    }
    RoundPeak peak = round.finish();
    checkFailure();
    return peak;
  }

  @Override
  public void close() throws Exception {
    if (sampler == null) {
      return;
    }
    sampler.interrupt();
    if (!stopped.await(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Resource sampler did not stop");
    }
    checkFailure();
  }

  private void sampleLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        Round round = currentRound.get();
        if (round != null) {
          round.sample();
        }
        Thread.sleep(SAMPLE_INTERVAL_MILLIS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      samplingFailure.compareAndSet(null, t);
    } finally {
      stopped.countDown();
    }
  }

  private void checkFailure() throws IOException {
    Throwable failure = samplingFailure.get();
    if (failure == null) {
      return;
    }
    if (failure instanceof IOException ioException) {
      throw ioException;
    }
    throw new IOException("Failed to sample DataNode resources", failure);
  }

  private ProcessSample readProcessSample() throws IOException {
    Path processDirectory = PROC.resolve(Long.toString(dataNodePid));
    String stat = Files.readString(processDirectory.resolve("stat"));
    int closingParenthesis = stat.lastIndexOf(") ");
    if (closingParenthesis < 0) {
      throw new IOException("Malformed /proc stat for DataNode PID " + dataNodePid);
    }
    String[] fields = stat.substring(closingParenthesis + 2).trim().split("\\s+");
    if (fields.length <= 12) {
      throw new IOException("Incomplete /proc stat for DataNode PID " + dataNodePid);
    }
    long cpuTicks = Long.parseLong(fields[11]) + Long.parseLong(fields[12]);
    long residentBytes = 0;
    for (String line : Files.readAllLines(processDirectory.resolve("status"))) {
      if (line.startsWith("VmRSS:")) {
        residentBytes = Long.parseLong(line.replaceAll("[^0-9]", "").trim()) * 1024;
        break;
      }
    }
    if (residentBytes <= 0) {
      throw new IOException("DataNode RSS unavailable for PID " + dataNodePid);
    }
    return new ProcessSample(System.nanoTime(), cpuTicks, residentBytes);
  }

  private static long readClockTicksPerSecond() throws IOException {
    Process process = new ProcessBuilder("getconf", "CLK_TCK").redirectErrorStream(true).start();
    try {
      if (!process.waitFor(5, TimeUnit.SECONDS)) {
        throw new IOException("getconf CLK_TCK timed out");
      }
      String output =
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      if (process.exitValue() != 0) {
        throw new IOException("getconf CLK_TCK failed: " + output);
      }
      long result = Long.parseLong(output);
      if (result <= 0) {
        throw new IOException("Invalid CLK_TCK: " + result);
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading CLK_TCK", e);
    } finally {
      process.destroyForcibly();
    }
  }

  private final class Round {
    private long lastCpuTicks;
    private long lastSampleNanos;
    private double peakCpuPercent = Double.NaN;
    private long peakResidentBytes;
    private boolean active = true;

    private synchronized void sample() throws IOException {
      if (!active) {
        return;
      }
      // Read and apply samples under the same lock so the final sample cannot overtake the sampler.
      ProcessSample sample = readProcessSample();
      peakResidentBytes = Math.max(peakResidentBytes, sample.residentBytes);
      long elapsedNanos = sample.nanoTime - lastSampleNanos;
      if (lastSampleNanos != 0) {
        // Keep the CPU baseline for short intervals: tick quantization can inflate a tail sample.
        if (elapsedNanos < TimeUnit.MILLISECONDS.toNanos(SAMPLE_INTERVAL_MILLIS)) {
          return;
        }
        long cpuTicks = sample.cpuTicks - lastCpuTicks;
        double cpuPercent =
            cpuTicks * 100.0 / clockTicksPerSecond / (elapsedNanos / 1_000_000_000.0);
        peakCpuPercent =
            Double.isNaN(peakCpuPercent) ? cpuPercent : Math.max(peakCpuPercent, cpuPercent);
      }
      lastCpuTicks = sample.cpuTicks;
      lastSampleNanos = sample.nanoTime;
    }

    private synchronized RoundPeak finish() throws IOException {
      try {
        sample();
        return new RoundPeak(peakCpuPercent, peakResidentBytes);
      } finally {
        active = false;
      }
    }
  }

  private record ProcessSample(long nanoTime, long cpuTicks, long residentBytes) {}

  record RoundPeak(double peakCpuPercent, long peakResidentBytes) {
    private static final RoundPeak UNAVAILABLE = new RoundPeak(Double.NaN, -1);

    String format() {
      if (this == UNAVAILABLE) {
        return "peakDataNodeCpu=unavailable, peakDataNodeRss=unavailable";
      }
      return String.format(
          "peakDataNodeCpu=%s, peakDataNodeRss=%.1f MiB",
          Double.isNaN(peakCpuPercent) ? "unavailable" : String.format("%.1f%%", peakCpuPercent),
          peakResidentBytes / 1024.0 / 1024.0);
    }
  }
}
