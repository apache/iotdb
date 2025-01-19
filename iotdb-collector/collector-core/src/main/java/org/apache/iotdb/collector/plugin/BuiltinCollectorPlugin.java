package org.apache.iotdb.collector.plugin;

import org.apache.iotdb.collector.plugin.builtin.processor.DoNothingProcessor;
import org.apache.iotdb.collector.plugin.builtin.sink.SessionSink;
import org.apache.iotdb.collector.plugin.builtin.source.HttpSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum BuiltinCollectorPlugin {

  // Sources
  HTTP_SOURCE("http-source", HttpSource.class),

  // Processors
  DO_NOTHING_PROCESSOR("do-nothing-processor", DoNothingProcessor.class),

  // Sinks
  IOTDB_SESSION_SINK("iotdb-session-sink", SessionSink.class);

  private final String collectorPluginName;
  private final Class<?> collectorPluginClass;
  private final String className;

  BuiltinCollectorPlugin(String collectorPluginName, Class<?> collectorPluginClass) {
    this.collectorPluginName = collectorPluginName;
    this.collectorPluginClass = collectorPluginClass;
    this.className = collectorPluginClass.getName();
  }

  public String getCollectorPluginName() {
    return collectorPluginName;
  }

  public Class<?> getCollectorPluginClass() {
    return collectorPluginClass;
  }

  public String getClassName() {
    return className;
  }

  public static final Set<String> SHOW_COLLECTOR_PLUGINS_BLACKLIST =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  // Sources
                  HTTP_SOURCE.getCollectorPluginName().toUpperCase(),
                  // Processors
                  DO_NOTHING_PROCESSOR.getCollectorPluginName().toUpperCase(),
                  // Sinks
                  IOTDB_SESSION_SINK.getCollectorPluginName().toUpperCase())));
}
