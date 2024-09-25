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

package org.apache.iotdb.commons.client.util;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.function.Consumer;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

// By recording the port range that has been used, the problem of brute force iterative query of
// available ports can be reduced. For example, if ports 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, and 14
// are used, they are recorded as 1-9, 11-12, 14, and the order is guaranteed.
public class IoTDBConnectorPortManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectorPortManager.class);

  public List<Pair<Integer, Integer>> getOccupiedPorts() {
    return occupiedPorts;
  }

  private final List<Pair<Integer, Integer>> occupiedPorts = new LinkedList<>();

  // Set the port boundaries
  private IoTDBConnectorPortManager() {
    occupiedPorts.add(new Pair<>(-1, -1));
    occupiedPorts.add(new Pair<>(65536, 65536));
  }

  public static final IoTDBConnectorPortManager INSTANCE = new IoTDBConnectorPortManager();

  // ===========================Iterator================================

  // Iterator can be used to iterate over undocumented ports
  public static class AvailablePortIterator implements Iterator<Integer> {
    private ListIterator<Pair<Integer, Integer>> occupiedPortsIterator;
    private ListIterator<Pair<Integer, Integer>> preOccupiedPortsIterator;
    private Iterator<Pair<Integer, Integer>> availableRangesIterator;
    private Pair<Integer, Integer> availableRange;
    private Pair<Integer, Integer> previousRange;
    private Pair<Integer, Integer> currentRange;
    private boolean hasNext = true;
    private int availablePort = -1;
    private int maxAvailablePort = -2;

    AvailablePortIterator(
        final List<Pair<Integer, Integer>> occupiedPorts,
        final List<Pair<Integer, Integer>> availableRanges) {
      if (occupiedPorts.size() <= 1 || availableRanges.isEmpty()) {
        hasNext = false;
        return;
      }
      this.occupiedPortsIterator = occupiedPorts.listIterator();
      this.preOccupiedPortsIterator = occupiedPorts.listIterator();
      this.availableRangesIterator = availableRanges.iterator();
      this.availableRange = availableRangesIterator.next();
      this.currentRange = occupiedPortsIterator.next();
      previousRange = currentRange;
      currentRange = occupiedPortsIterator.next();
      preOccupiedPortsIterator.next();
    }

    @Override
    public boolean hasNext() {
      if (!hasNext) {
        return false;
      }
      if (availablePort <= maxAvailablePort) {
        return true;
      }
      if (availablePort != -1) {
        if (availableRange.getRight() <= currentRange.right) {
          if (!updateAvailablePort()) {
            return hasNext = false;
          }
        } else {
          if (!updateCurrentRanges()) {
            return hasNext = false;
          }
        }
      }
      out:
      while (true) {
        if (currentRange.getRight() <= availableRange.getLeft()) {
          if (!updateCurrentRanges()) {
            return hasNext = false;
          }
          continue;
        }
        if (previousRange.getLeft() >= availableRange.getRight()) {
          if (!updateAvailablePort()) {
            return hasNext = false;
          }
          continue;
        }
        final int max = Math.min(availableRange.getRight(), currentRange.getLeft() - 1);
        final int min = Math.max(availableRange.getLeft(), previousRange.getRight() + 1);
        if (max < min) {
          if (availableRange.getRight() <= currentRange.right) {
            if (!updateAvailablePort()) {
              return hasNext = false;
            }
            continue;
          }
          if (!updateCurrentRanges()) {
            return hasNext = false;
          }
          continue;
        }
        availablePort = min;
        maxAvailablePort = max;
        return hasNext = true;
      }
    }

    private boolean updateAvailablePort() {
      if (!availableRangesIterator.hasNext()) {
        return false;
      }
      availableRange = availableRangesIterator.next();
      return true;
    }

    private boolean updateCurrentRanges() {
      if (!occupiedPortsIterator.hasNext()) {
        return false;
      }
      previousRange = currentRange;
      currentRange = occupiedPortsIterator.next();
      preOccupiedPortsIterator.next();
      return true;
    }

    @Override
    public Integer next() {
      if (availablePort > maxAvailablePort) {
        if (!hasNext()) {
          throw new PipeConnectionException("No more available ports to iterate.");
        }
      }
      final int value = this.availablePort;
      this.availablePort++;
      return value;
    }

    public void updateOccupiedPort() {
      if (availablePort == -1) {
        throw new IllegalStateException("Available port not initialized.");
      }
      final int value = availablePort - 1;
      if (value == previousRange.getRight() + 1 && currentRange.getLeft() - 1 == value) {
        previousRange.setRight(currentRange.getRight());
        occupiedPortsIterator.remove();
        return;
      }
      if (value == previousRange.getRight() + 1) {
        previousRange.setRight(value);
        return;
      }
      if (value == currentRange.getLeft() - 1) {
        currentRange.setLeft(value);
        return;
      }
      preOccupiedPortsIterator.add(new Pair<>(value, value));
    }
  }

  // ===========================add and release================================

  public boolean addPortIfAvailable(final int candidatePort) {
    synchronized (occupiedPorts) {
      if (occupiedPorts.size() == 1 || occupiedPorts.isEmpty()) {
        return false;
      }

      ListIterator<Pair<Integer, Integer>> occupiedPortsIterator = occupiedPorts.listIterator();
      ListIterator<Pair<Integer, Integer>> previousIterator = occupiedPorts.listIterator();
      Pair<Integer, Integer> previousRange = null;
      Pair<Integer, Integer> currentRange = occupiedPortsIterator.next();
      while (occupiedPortsIterator.hasNext()) {
        previousRange = previousIterator.next();
        currentRange = occupiedPortsIterator.next();
        if (currentRange.getRight() <= candidatePort) {
          continue;
        }
        if (previousRange.getLeft() >= candidatePort) {
          break;
        }
        if (previousRange.getRight() >= candidatePort || currentRange.getLeft() <= candidatePort) {
          return false;
        }
        if (candidatePort == previousRange.getRight() + 1
            && currentRange.getLeft() - 1 == candidatePort) {
          previousRange.setRight(currentRange.getRight());
          occupiedPortsIterator.remove();
          return true;
        }

        if (candidatePort == previousRange.getRight() + 1) {
          previousRange.setRight(candidatePort);
          return true;
        }

        if (candidatePort == currentRange.getLeft() - 1) {
          currentRange.setLeft(candidatePort);
          return true;
        }

        previousIterator.add(new Pair<>(candidatePort, candidatePort));
        return true;
      }
    }
    return false;
  }

  public void releaseUsedPort(final int port) {
    synchronized (occupiedPorts) {
      if (occupiedPorts.isEmpty()) {
        return;
      }
      ListIterator<Pair<Integer, Integer>> iterator = occupiedPorts.listIterator();
      Pair<Integer, Integer> cur = null;
      while (iterator.hasNext()) {
        cur = iterator.next();
        if (port > cur.getRight()) {
          continue;
        }
        if (port < cur.getLeft()) {
          break;
        }
        if (cur.getLeft().equals(cur.getRight())) {
          iterator.remove();
          break;
        }
        if (cur.getLeft() == port) {
          cur.setLeft(port + 1);
          break;
        }
        if (cur.getRight() == port) {
          cur.setRight(port - 1);
          break;
        }
        iterator.add(new Pair<>(port + 1, cur.getRight()));
        cur.setRight(port - 1);
        break;
      }
    }
  }

  // ===========================bing================================

  public void bingPort(
      final int minSendPortRange,
      final int maxSendPortRange,
      final List<Integer> candidatePorts,
      final Consumer<Integer, Exception> consumer) {
    synchronized (occupiedPorts) {
      AvailablePortIterator portIterator =
          createAvailablePortIterator(minSendPortRange, maxSendPortRange, candidatePorts);
      boolean portFound = false;
      Exception lastException = null;
      while (portIterator.hasNext()) {
        try {
          consumer.accept(portIterator.next());
          portIterator.updateOccupiedPort();
          portFound = true;
          break;
        } catch (Exception e) {
          lastException = e;
        }
      }
      if (!portFound) {
        String exceptionMessage =
            String.format(
                "Failed to find an available send port. Custom send port is defined. "
                    + "No ports are available in the candidate list [%s] or within the range %d to %d.",
                candidatePorts, minSendPortRange, maxSendPortRange);
        LOGGER.warn(exceptionMessage, lastException);
        throw new PipeConnectionException(exceptionMessage);
      }
    }
  }

  @TestOnly
  public void resetPortManager() {
    synchronized (occupiedPorts) {
      occupiedPorts.clear();
      occupiedPorts.add(new Pair<>(-1, -1));
      occupiedPorts.add(new Pair<>(65536, 65536));
    }
  }

  public AvailablePortIterator createAvailablePortIterator(
      final int minSendPortRange, final int maxSendPortRange, final List<Integer> candidatePorts) {
    return new AvailablePortIterator(
        occupiedPorts, createPortRanges(candidatePorts, minSendPortRange, maxSendPortRange));
  }

  private List<Pair<Integer, Integer>> createPortRanges(
      final List<Integer> candidatePorts, final int minSendPortRange, final int maxSendPortRange) {
    List<Pair<Integer, Integer>> range = new LinkedList<>();
    if (candidatePorts.isEmpty()) {
      range.add(new Pair<>(minSendPortRange, maxSendPortRange));
      return range;
    }
    Iterator<Integer> candidatePortIterator = candidatePorts.iterator();
    int tempValue = -1;
    while (candidatePortIterator.hasNext()) {
      int value = candidatePortIterator.next();
      if (value >= minSendPortRange) {
        tempValue = value;
        break;
      }
      addPair(range, value);
    }
    addPair(range, minSendPortRange, maxSendPortRange);
    if (tempValue != -1) {
      if (tempValue > maxSendPortRange) {
        addPair(range, tempValue);
      }
    }

    while (candidatePortIterator.hasNext()) {
      int value = candidatePortIterator.next();
      if (value <= maxSendPortRange) {
        continue;
      }
      addPair(range, value);
    }
    return range;
  }

  private void addPair(List<Pair<Integer, Integer>> range, int value) {
    if (!range.isEmpty() && range.get(range.size() - 1).getRight() == value - 1) {
      range.get(range.size() - 1).setRight(value);
    } else {
      range.add(new Pair<>(value, value));
    }
  }

  private void addPair(List<Pair<Integer, Integer>> range, int minValue, int maxValue) {
    if (!range.isEmpty() && range.get(range.size() - 1).getRight() == minValue - 1) {
      range.get(range.size() - 1).setRight(maxValue);
    } else {
      range.add(new Pair<>(minValue, maxValue));
    }
  }
}
