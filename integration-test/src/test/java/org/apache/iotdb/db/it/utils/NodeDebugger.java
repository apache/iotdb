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

package org.apache.iotdb.db.it.utils;

import com.sun.jdi.Bootstrap;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StackFrame;
import com.sun.jdi.StringReference;
import com.sun.jdi.ThreadReference;
import com.sun.jdi.VMDisconnectedException;
import com.sun.jdi.Value;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.connect.AttachingConnector;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.MethodEntryEvent;
import com.sun.jdi.event.MethodExitEvent;
import com.sun.jdi.event.VMStartEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.ClassPrepareRequest;
import com.sun.jdi.request.EventRequest;
import com.sun.jdi.request.MethodEntryRequest;
import com.sun.jdi.request.MethodExitRequest;
import org.junit.Assert;

import java.net.ConnectException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Pauses only the node thread executing a selected method. Other threads (including the active
 * scanner) keep running. All control stays in the test JVM; no product hooks or agent bytecode are
 * installed. Closing the debugger releases a paused thread even when an assertion fails.
 */
public final class NodeDebugger implements AutoCloseable {
  private final VirtualMachine vm;
  private final String className;
  private final String methodName;
  private final boolean observeMethodEntries;
  private final Set<ReferenceType> breakpointTypes = new HashSet<>();
  private EventSet suspended;
  private ThreadReference pausedThread;

  public NodeDebugger(int port, String className, String methodName) throws Exception {
    this(port, className, methodName, false);
  }

  public NodeDebugger(int port, String className, String methodName, boolean observeMethodEntries)
      throws Exception {
    this.className = className;
    this.methodName = methodName;
    this.observeMethodEntries = observeMethodEntries;
    final AttachingConnector connector =
        Bootstrap.virtualMachineManager().attachingConnectors().stream()
            .filter(candidate -> candidate.name().equals("com.sun.jdi.SocketAttach"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    final Map<String, Connector.Argument> arguments = connector.defaultArguments();
    arguments.get("hostname").setValue("127.0.0.1");
    arguments.get("port").setValue(Integer.toString(port));
    arguments.get("timeout").setValue("10000");
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    VirtualMachine connected;
    while (true) {
      try {
        connected = connector.attach(arguments);
        break;
      } catch (ConnectException e) {
        if (System.nanoTime() >= deadline) {
          throw e;
        }
        // ProcessBuilder.start returns before the suspended VM has opened its JDWP listener.
        TimeUnit.MILLISECONDS.sleep(100);
      }
    }
    vm = connected;
    try {
      if (observeMethodEntries) {
        installMethodEntryRequest();
      } else {
        final ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
        prepare.addClassFilter(className);
        prepare.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        prepare.enable();
        for (ReferenceType type : vm.classesByName(className)) {
          if (type.isPrepared()) {
            installBreakpoint(type);
          }
        }
      }
    } catch (Exception | AssertionError e) {
      vm.dispose();
      throw e;
    }
  }

  private void installMethodEntryRequest() {
    final MethodEntryRequest entry = vm.eventRequestManager().createMethodEntryRequest();
    entry.addClassFilter(className);
    entry.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
    entry.enable();
  }

  /** Releases only the initial VMStart suspension after all requests have been installed. */
  public void resumeStartup() throws Exception {
    suspended = vm.eventQueue().remove(10000);
    Assert.assertNotNull("Suspended startup must produce a VMStart event", suspended);
    Assert.assertTrue(suspended.stream().anyMatch(event -> event instanceof VMStartEvent));
    resume();
  }

  private void installBreakpoint(ReferenceType type) {
    if (!breakpointTypes.add(type)) {
      return;
    }
    Assert.assertEquals(className + "." + methodName, 1, type.methodsByName(methodName).size());
    final BreakpointRequest breakpoint =
        vm.eventRequestManager()
            .createBreakpointRequest(type.methodsByName(methodName).get(0).location());
    breakpoint.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
    breakpoint.enable();
  }

  public StackFrame await(Predicate<StackFrame> condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < deadline) {
      final EventSet events =
          vm.eventQueue()
              .remove(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
      if (events == null) {
        break;
      }
      // Remember ownership before inspecting a frame: failed predicates must also release it.
      suspended = events;
      for (Event event : events) {
        if (event instanceof ClassPrepareEvent && !observeMethodEntries) {
          installBreakpoint(((ClassPrepareEvent) event).referenceType());
        } else if (event instanceof MethodEntryEvent
            && ((MethodEntryEvent) event).method().name().equals(methodName)) {
          final ThreadReference thread = ((MethodEntryEvent) event).thread();
          final StackFrame frame = thread.frame(0);
          if (condition.test(frame)) {
            pausedThread = thread;
            return frame;
          }
        } else if (event instanceof BreakpointEvent) {
          final StackFrame frame = ((BreakpointEvent) event).thread().frame(0);
          if (condition.test(frame)) {
            pausedThread = ((BreakpointEvent) event).thread();
            return frame;
          }
        } else if (event instanceof MethodExitEvent
            && ((MethodExitEvent) event).method().name().equals(methodName)) {
          final StackFrame frame = ((MethodExitEvent) event).thread().frame(0);
          if (condition.test(frame)) {
            // Resume must let cleanup and the SQL response run without another exit pause.
            event.request().disable();
            return frame;
          }
        }
      }
      resume();
    }
    throw new AssertionError("Did not reach " + className + "." + methodName);
  }

  public void resume() {
    if (suspended != null) {
      suspended.resume();
      suspended = null;
    }
  }

  /** Changes an entry pause into an exit pause on the same method and connection. */
  public void pauseOnExit() {
    vm.eventRequestManager()
        .deleteEventRequests(
            new java.util.ArrayList<>(vm.eventRequestManager().breakpointRequests()));
    vm.eventRequestManager()
        .deleteEventRequests(
            new java.util.ArrayList<>(vm.eventRequestManager().classPrepareRequests()));
    vm.eventRequestManager()
        .deleteEventRequests(
            new java.util.ArrayList<>(vm.eventRequestManager().methodEntryRequests()));
    final MethodExitRequest exit = vm.eventRequestManager().createMethodExitRequest();
    exit.addClassFilter(className);
    Assert.assertNotNull("An entry pause is required before selecting its exit", pausedThread);
    exit.addThreadFilter(pausedThread);
    exit.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
    exit.enable();
  }

  /** Reads java.io.File's path without invoking code in the paused node. */
  public static String filePath(Value value) {
    final ObjectReference file = (ObjectReference) value;
    return ((StringReference) file.getValue(file.referenceType().fieldByName("path"))).value();
  }

  @Override
  public void close() {
    try {
      resume();
      vm.dispose();
    } catch (VMDisconnectedException expectedAfterCrash) {
      // A forced node stop is the operation under test.
    }
  }
}
