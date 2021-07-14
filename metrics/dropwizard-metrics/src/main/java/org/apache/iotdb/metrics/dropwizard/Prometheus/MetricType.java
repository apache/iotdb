package org.apache.iotdb.metrics.dropwizard.Prometheus;

enum MetricType {
    COUNTER     ("counter"),
    GAUGE       ("gauge"),
    SUMMARY     ("summary"),
    HISTOGRAM   ("histogram"),
    UNTYPED     ("untyped");

    private final String text;

    MetricType(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

}
