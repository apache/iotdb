package org.apache.iotdb.metrics.dropwizard.Prometheus;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class Pushgateway implements PrometheusSender {

    private static final int SECONDS_PER_MILLISECOND = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(Pushgateway.class);

    private final String url;
    private final String job;

    private volatile HttpURLConnection connection = null;
    private PrometheusTextWriter writer;
    private DropwizardMetricsExporter exporter;

    public Pushgateway(String url) {
        this(url, "prometheus");
    }

    public Pushgateway(String url, String job) {
        this.url = url;
        this.job = job;
    }

    @Override
    public void close() throws IOException {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            LOG.error("Error closing writer", e);
        } finally {
            this.writer = null;
            this.exporter = null;
        }

        int response = connection.getResponseCode();
        if (response != HttpURLConnection.HTTP_ACCEPTED) {
            throw new IOException("Response code from " + url + " was " + response);
        }
        connection.disconnect();
        this.connection = null;
    }

    @Override
    public void connect() throws IOException {
        if (!isConnected()) {
            String targetUrl = url + "/metrics/job/" + URLEncoder.encode(job, StandardCharsets.UTF_8.name());
            HttpURLConnection conn = (HttpURLConnection) new URL(targetUrl).openConnection();
            conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, TextFormat.REQUEST_CONTENT_TYPE);
            conn.setDoOutput(true);
            conn.setRequestMethod(HttpMethod.POST);

            conn.setConnectTimeout(10 * SECONDS_PER_MILLISECOND);
            conn.setReadTimeout(10 * SECONDS_PER_MILLISECOND);
            conn.connect();
            this.writer = new PrometheusTextWriter(new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8)));
            this.exporter = new DropwizardMetricsExporter(writer);
            this.connection = conn;
        }
    }

    @Override
    public void sendGauge(String name, Gauge<?> gauge) throws IOException {
        exporter.writeGauge(name, gauge);
    }

    @Override
    public void sendCounter(String name, Counter counter) throws IOException {
        exporter.writeCounter(name, counter);
    }

    @Override
    public void sendHistogram(String name, Histogram histogram) throws IOException {
        exporter.writeHistogram(name, histogram);
    }

    @Override
    public void sendMeter(String name, Meter meter) throws IOException {
        exporter.writeMeter(name, meter);
    }

    @Override
    public void sendTimer(String name, Timer timer) throws IOException {
        exporter.writeTimer(name, timer);
    }

    @Override
    public void flush() throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public boolean isConnected() {
        return connection != null;
    }

}
