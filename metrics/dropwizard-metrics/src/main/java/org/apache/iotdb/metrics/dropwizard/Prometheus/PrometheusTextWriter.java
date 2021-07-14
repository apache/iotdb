package org.apache.iotdb.metrics.dropwizard.Prometheus;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

class PrometheusTextWriter extends FilterWriter {

    public PrometheusTextWriter(Writer out) {
        super(out);
    }

    public void writeHelp(String name, String value) throws IOException {
        write("# HELP ");
        write(name);
        write(' ');
        writeEscapedHelp(value);
        write('\n');
    }

    public void writeType(String name, MetricType type) throws IOException {
        write("# TYPE ");
        write(name);
        write(' ');
        write(type.getText());
        write('\n');
    }

    public void writeSample(String name, Map<String, String> labels, double value) throws IOException {
        write(name);
        if (labels.size() > 0) {
            write('{');
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                write(entry.getKey());
                write("=\"");
                writeEscapedLabelValue(entry.getValue());
                write("\",");
            }
            write('}');
        }
        write(' ');
        write(doubleToGoString(value));
        write('\n');
    }

    private void writeEscapedHelp(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    append("\\\\");
                    break;
                case '\n':
                    append("\\n");
                    break;
                default:
                    append(c);
            }
        }
    }

    private void writeEscapedLabelValue(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    append("\\\\");
                    break;
                case '\"':
                    append("\\\"");
                    break;
                case '\n':
                    append("\\n");
                    break;
                default:
                    append(c);
            }
        }
    }

    private static String doubleToGoString(double d) {
        if (d == Double.POSITIVE_INFINITY) {
          return "+Inf";
        }
        if (d == Double.NEGATIVE_INFINITY) {
          return "-Inf";
        }
        if (Double.isNaN(d)) {
          return "NaN";
        }
        return Double.toString(d);
      }

}
