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

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class PortUtilizationManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PortUtilizationManager.class);

  private final List<Pair<Integer, Integer>> occupiedPorts = new LinkedList<>();

  private PortUtilizationManager() {
    occupiedPorts.add(new Pair<>(1023, 1023));
    occupiedPorts.add(new Pair<>(65536, 65536));
  }

  public static final PortUtilizationManager INSTANCE = new PortUtilizationManager();

  public Integer addIfPortAvailable(int candidatePort) {
    synchronized (occupiedPorts) {
      if (occupiedPorts.size() == 1 || occupiedPorts.isEmpty()) {
        return null;
      }
      ListIterator<Pair<Integer, Integer>> occupiedPortsIterator = occupiedPorts.listIterator();
      Pair<Integer, Integer> previousRange = null;
      Pair<Integer, Integer> currentRange = occupiedPortsIterator.next();
      while (occupiedPortsIterator.hasNext()) {
        previousRange = currentRange;
        currentRange = occupiedPortsIterator.next();
        if (currentRange.getRight() <= candidatePort) {
          continue;
        }
        if (previousRange.getLeft() >= candidatePort) {
          break;
        }
        if (previousRange.getRight() >= candidatePort && currentRange.getLeft() <= candidatePort) {
          return null;
        }
        Integer availablePort = checkPortAvailability(candidatePort);
        if (availablePort == null) {
          return null;
        }

        if (availablePort == previousRange.getRight() + 1
            && currentRange.getLeft() - 1 == availablePort) {
          previousRange.setRight(availablePort);
          occupiedPortsIterator.remove();
          return availablePort;
        }

        if (availablePort == previousRange.getRight() + 1) {
          previousRange.setRight(availablePort);
          return availablePort;
        }

        if (availablePort == currentRange.getLeft() - 1) {
          currentRange.setLeft(availablePort);
          return availablePort;
        }

        occupiedPortsIterator.add(new Pair<>(availablePort, availablePort));
        return availablePort;
      }
    }
    return null;
  }

  private Integer addIfPortAvailable(List<Integer> candidatePorts) {
    synchronized (occupiedPorts) {
      if (occupiedPorts.size() == 1 || occupiedPorts.isEmpty() || candidatePorts.isEmpty()) {
        return null;
      }

      ListIterator<Pair<Integer, Integer>> occupiedPortsIterator = occupiedPorts.listIterator();
      Iterator<Integer> integerIterator = candidatePorts.iterator();
      Pair<Integer, Integer> previousRange = null;
      Pair<Integer, Integer> currentRange = occupiedPortsIterator.next();
      Integer candidatePort = integerIterator.next();
      out:
      while (occupiedPortsIterator.hasNext()) {
        previousRange = currentRange;
        currentRange = occupiedPortsIterator.next();

        while (true) {
          if (currentRange.getRight() <= candidatePort) {
            continue out;
          }
          while (previousRange.getLeft() >= candidatePort) {
            if (!integerIterator.hasNext()) {
              return null;
            }
            candidatePort = integerIterator.next();
          }
          if (candidatePort < currentRange.getLeft()
              && candidatePort > previousRange.getRight()
              && checkPortAvailability(candidatePort) != null) {
            break;
          }
          if (!integerIterator.hasNext()) {
            return null;
          }
          candidatePort = integerIterator.next();
        }

        if (candidatePort == previousRange.getRight() + 1
            && currentRange.getLeft() - 1 == candidatePort) {
          previousRange.setRight(candidatePort);
          occupiedPortsIterator.remove();
          return candidatePort;
        }

        if (candidatePort == previousRange.getRight() + 1) {
          previousRange.setRight(candidatePort);
          return candidatePort;
        }

        if (candidatePort == currentRange.getLeft() - 1) {
          currentRange.setLeft(candidatePort);
          return candidatePort;
        }

        occupiedPortsIterator.add(new Pair<>(candidatePort, candidatePort));
        return candidatePort;
      }
    }
    return null;
  }

  private Integer addIfPortAvailable(int minSendPortRange, int maxSendPortRange) {
    synchronized (occupiedPorts) {
      if (occupiedPorts.size() == 1 || occupiedPorts.isEmpty()) {
        return null;
      }

      ListIterator<Pair<Integer, Integer>> occupiedPortsIterator = occupiedPorts.listIterator();
      Pair<Integer, Integer> previousRange = null;
      Pair<Integer, Integer> currentRange = occupiedPortsIterator.next();
      while (occupiedPortsIterator.hasNext()) {
        previousRange = currentRange;
        currentRange = occupiedPortsIterator.next();
        if (currentRange.getRight() <= minSendPortRange) {
          continue;
        }
        if (previousRange.getLeft() >= maxSendPortRange) {
          break;
        }

        int max = Math.min(maxSendPortRange, currentRange.getLeft() - 1);
        int min = Math.max(minSendPortRange, previousRange.getRight() + 1);
        if (max < min) {
          return null;
        }
        Integer availablePort = findAvailablePort(min, max);
        if (availablePort == null) {
          continue;
        }

        if (availablePort == previousRange.getRight() + 1
            && currentRange.getLeft() - 1 == availablePort) {
          previousRange.setRight(availablePort);
          occupiedPortsIterator.remove();
          return availablePort;
        }

        if (availablePort == previousRange.getRight() + 1) {
          previousRange.setRight(availablePort);
          return availablePort;
        }

        if (availablePort == currentRange.getLeft() - 1) {
          currentRange.setLeft(availablePort);
          return availablePort;
        }

        occupiedPortsIterator.add(new Pair<>(availablePort, availablePort));
        return availablePort;
      }
    }
    return null;
  }

  private Integer findAvailablePort(int min, int max) {
    for (int i = min; i <= max; i++) {
      try {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
          sm.checkListen(i);
        }
        return i;
      } catch (Exception ignored) {
      }
    }
    return null;
  }

  private Integer checkPortAvailability(int port) {
    try {
      SecurityManager sm = System.getSecurityManager();
      if (sm != null) {
        sm.checkListen(port);
      }
      return port;
    } catch (Exception ignored) {
    }
    return null;
  }

  public void releasePortIfUsed(int port) {
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
        if (cur.getLeft() == port) {
          if (cur.getLeft().equals(cur.getRight())) {
            iterator.remove();
            break;
          }
          cur.setLeft(cur.getLeft() + 1);
        }

        if (cur.getRight() == port) {
          if (cur.getLeft().equals(cur.getRight())) {
            iterator.remove();
            break;
          }
          cur.setRight(cur.getRight() - 1);
          break;
        }
        cur.setRight(port - 1);
        iterator.add(new Pair<>(port + 1, cur.getRight()));
        break;
      }
    }
  }

  public Integer findAvailablePort(
      int minSendPortRange, int maxSendPortRange, List<Integer> candidatePorts) {
    Integer port = addIfPortAvailable(candidatePorts);
    if (port != null) {
      return port;
    } else {
      LOGGER.debug("No available port found in the candidate list, trying range.");
    }
    port = addIfPortAvailable(minSendPortRange, maxSendPortRange);
    if (port == null) {
      String exceptionMessage =
          String.format(
              "Failed to find an available port within the range %d to %d.",
              minSendPortRange, maxSendPortRange);
      LOGGER.warn(exceptionMessage);
    }
    return port;
  }
}
