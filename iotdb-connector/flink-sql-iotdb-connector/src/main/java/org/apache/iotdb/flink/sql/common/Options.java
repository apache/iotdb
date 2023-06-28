package org.apache.iotdb.flink.sql.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Options {
    public static final ConfigOption<String> NODE_URLS = ConfigOptions
            .key("nodeUrls")
            .stringType()
            .defaultValue("127.0.0.1:6667");
    public static final ConfigOption<String> USER = ConfigOptions
            .key("user")
            .stringType()
            .defaultValue("root");
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .defaultValue("root");
    public static final ConfigOption<String> DEVICE = ConfigOptions
            .key("device")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Boolean> ALIGNED = ConfigOptions
            .key("aligned")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .intType()
            .defaultValue(-1);
    public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC = ConfigOptions
            .key("lookup.cache.ttl-sec")
            .intType()
            .defaultValue(-1);
    public static final ConfigOption<Long> SCAN_BOUNDED_LOWER_BOUND = ConfigOptions
            .key("scan.bounded.lower-bound")
            .longType()
            .defaultValue(-1L);
    public static final ConfigOption<Long> SCAN_BOUNDED_UPPER_BOUND = ConfigOptions
            .key("scan.bounded.upper-bound")
            .longType()
            .defaultValue(-1L);
}
