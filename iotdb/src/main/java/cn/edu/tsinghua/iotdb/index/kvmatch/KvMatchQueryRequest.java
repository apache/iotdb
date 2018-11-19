package cn.edu.tsinghua.iotdb.index.kvmatch;


import cn.edu.tsinghua.iotdb.index.QueryRequest;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

/**
 * An instance of this class represents a query request with specific parameters.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryRequest extends QueryRequest {

    private Path queryPath;

    private long queryStartTime;

    private long queryEndTime;

    private double epsilon;

    private double alpha;

    private double beta;

    /**
     * Private constructor used by the nested Builder class.
     *
     * @param builder builder used to create this query request
     */
    private KvMatchQueryRequest(final Builder builder) {
        super(builder.columnPath, builder.startTime, builder.endTime);
        this.epsilon = builder.epsilon;
        this.alpha = builder.alpha;
        this.beta = builder.beta;
        this.queryPath = builder.queryPath;
        this.queryStartTime = builder.queryStartTime;
        this.queryEndTime = builder.queryEndTime;
    }

    /**
     * Returns a {@link KvMatchQueryRequest.Builder} to create an {@link KvMatchQueryRequest} using descriptive methods.
     *
     * @return a new {@link KvMatchQueryRequest.Builder} instance
     */
    public static KvMatchQueryRequest.Builder builder(Path columnPath, Path queryPath, long queryStartTime, long queryEndTime, double epsilon) {
        return new Builder(columnPath, queryPath, queryStartTime, queryEndTime, epsilon);
    }

    public Path getQueryPath() {
        return queryPath;
    }

    public void setQueryPath(Path queryPath) {
        this.queryPath = queryPath;
    }

    public long getQueryStartTime() {
        return queryStartTime;
    }

    public void setQueryStartTime(long queryStartTime) {
        this.queryStartTime = queryStartTime;
    }

    public long getQueryEndTime() {
        return queryEndTime;
    }

    public void setQueryEndTime(long queryEndTime) {
        this.queryEndTime = queryEndTime;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public double getBeta() {
        return beta;
    }

    public void setBeta(double beta) {
        this.beta = beta;
    }

    /**
     * A nested builder class to create <code>KvMatchQueryRequest</code> instances using descriptive methods.
     * <p>
     * Example usage:
     * <pre>
     * KvMatchQueryRequest queryRequest = KvMatchQueryRequest.builder(columnPath, querySeries, epsilon)
     *                                                       .alpha(1.0)
     *                                                       .beta(0.0)
     *                                                       .startTime(1500350823)
     *                                                       .endTime(1500350823)
     *                                                       .build();
     * </pre>
     */
    public static final class Builder {

        private Path columnPath;

        private long startTime;

        private long endTime;

        private Path queryPath;

        private long queryStartTime;

        private long queryEndTime;

        private double epsilon;

        private double alpha;

        private double beta;

        /**
         * Constructs a new <code>Builder</code> with the minimum
         * required parameters for an <code>KvMatchQueryRequest</code> instance.
         *
         * @param columnPath     the column path request to query
         * @param queryPath      the column path used to extract pattern series
         * @param queryStartTime the start time of pattern series in query path
         * @param queryEndTime   the end time of pattern series in query path
         * @param epsilon        the distance threshold
         */
        private Builder(Path columnPath, Path queryPath, long queryStartTime, long queryEndTime, double epsilon) throws IllegalArgumentException {
            this.columnPath = columnPath;
            this.queryPath = queryPath;
            this.queryStartTime = queryStartTime;
            this.queryEndTime = queryEndTime;
            this.epsilon = epsilon;
            this.alpha = 1.0;
            this.beta = 0.0;
            this.startTime = 0;
            this.endTime = Long.MAX_VALUE;
        }

        /**
         * Sets the parameter alpha for the query request
         *
         * @param alpha the parameter alpha for the query request
         * @return this builder, to allow method chaining
         */
        public Builder alpha(final double alpha) {
            this.alpha = alpha;
            return this;
        }

        /**
         * Sets the parameter beta for the query request
         *
         * @param beta the parameter alpha for the query request
         * @return this builder, to allow method chaining
         */
        public Builder beta(final double beta) {
            this.beta = beta;
            return this;
        }

        /**
         * Sets the start time for the query request
         *
         * @param startTime the start time for the query request
         * @return this builder, to allow method chaining
         */
        public Builder startTime(final long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the end time for the query request
         *
         * @param endTime the end time for the query request
         * @return this builder, to allow method chaining
         */
        public Builder endTime(final long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Constructs an {@link KvMatchQueryRequest} with the values declared by this {@link KvMatchQueryRequest.Builder}.
         *
         * @return the new {@link KvMatchQueryRequest}
         * @throws IllegalArgumentException if either required arguments is illegal or has been set
         */
        public KvMatchQueryRequest build() {
            if (columnPath == null || queryPath == null || epsilon < 0 ||
                    alpha < 1.0 || beta < 0 || startTime > endTime || queryStartTime > queryEndTime) {
                throw new IllegalArgumentException("The given query request is not valid!");
            }
            return new KvMatchQueryRequest(this);
        }
    }
}
