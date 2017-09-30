package io.whitfin.elasticsearch.bulk;

import io.whitfin.elasticsearch.bulk.lifecycle.NoopLifecycle;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link BulkOperator} controls batch execution against Elasticsearch via
 * the Elasticsearch Bulk API. It contains control over concurrency, throttling
 * and lifecycle monitoring.
 *
 * To create an operator, you must go via the {@link BulkOperator.Builder} in
 * order to get sane defaults and validation.
 *
 * @see <a href="Elasticsearch Bulk API">https://www.elastic.co/guide/en/elasticsearch/reference/5.4/docs-bulk.html</a>
 */
public class BulkOperator implements Closeable {

    /**
     * Internal counter to use to generate execution identifiers.
     */
    private static final AtomicLong IDENTIFIERS = new AtomicLong();

    /**
     * Static header to use on all bulk requests, per the Elasticsearch API.
     */
    private static final Header ND_JSON_HEADER = new BasicHeader("content-type", "application/x-ndjson");

    /**
     * Static empty map to send as query parameters alongside requests.
     */
    private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

    /**
     * The lifecycle associated with this operator.
     */
    private final BulkLifecycle lifecycle;

    /**
     * Represents an upper bound on the actions to buffer.
     */
    private final Integer maxActions;

    /**
     * The client instance used to talk to Elasticsearch.
     */
    private final RestClient client;

    /**
     * Our scheduling service to allow flushing on interval.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * Our future to store references to scheduled executions.
     */
    private final ScheduledFuture scheduledFuture;

    /**
     * Our concurrency controller to limit access.
     */
    private final Semaphore semaphore;

    /**
     * Represents the current number of items to execute.
     */
    private volatile Integer current;

    /**
     * Represents whether this operator has been closed.
     */
    private Boolean closed;

    /**
     * An operation builder to buffer actions into.
     */
    private BulkOperation.Builder operation;

    /**
     * Initializes an operator from a {@link Builder} instance
     * to ensure that we have an expected state provided.
     *
     * @param builder the {@link Builder} to create from.
     */
    private BulkOperator(Builder builder) {
        // copy all basic builder values
        this.client = builder.client;
        this.lifecycle = builder.lifecycle;
        this.maxActions = builder.maxActions;

        // create our semaphore to control our concurrency
        this.semaphore = new Semaphore(builder.concurrency);

        // set initial states
        this.current = 0;
        this.closed = false;

        // begin our bulk operation creation via builder
        this.operation = BulkOperation.builder();

        // if we have an interval configured
        if (builder.interval != null) {
            // set up the new scheduled pool with a single threads
            this.scheduler = Executors.newScheduledThreadPool(1);

            // schedule our flush to happen on the provided interval
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        flush();
                    }
                },
                builder.interval,
                builder.interval,
                TimeUnit.MILLISECONDS
            );
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    /**
     * Adds new {@link BulkAction} instances to the internal
     * bulk operation builder.
     *
     * @param bulkActions
     *      the {@link BulkAction} instances to add.
     * @return
     *      the {@link BulkOperator} instance for chaining.
     */
    @SuppressWarnings("UnusedReturnValue")
    public synchronized BulkOperator add(BulkAction... bulkActions) {
        // short-circuit if closed
        if (this.closed) {
            throw new IllegalStateException("BulkOperator already closed");
        }

        // add the action
        this.current++;
        this.operation.addAction(bulkActions);

        // check for action count overages
        if (this.maxActions != null && this.current >= this.maxActions) {
            flush();
        }

        // chaining
        return this;
    }

    /**
     * Flushes all actions contained in the current buffer.
     * <p>
     * This uses a synchronized lock to ensure that the internal operation
     * is reset without causing any potential race conditions.
     * <p>
     * The internal semaphore is used to control concurrency, by waiting
     * for an acquired lock before executing the request internally.
     */
    @SuppressWarnings("WeakerAccess")
    public void flush() {
        // optimal short-circuit if we can
        if (this.closed || this.current == 0) {
            return;
        }

        // synchronize with the add method
        BulkOperation bulkOperation;
        synchronized (BulkOperator.this) {
            bulkOperation = this.operation.build();
            this.current = 0;
            this.operation = BulkOperation.builder();
        }

        try {
            // throttle concurrency
            this.semaphore.acquire();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // create a new identifier for this execution
        long bulkId = IDENTIFIERS.incrementAndGet();

        // create request listeners and convert the payload to a HttpEntity as JSON
        ResponseListener responseListener = new BulkResponseListener(bulkId, bulkOperation);
        HttpEntity entity = new NStringEntity(bulkOperation.payload(), ContentType.APPLICATION_JSON);

        // notify before listener and begin async request
        this.lifecycle.beforeBulk(bulkId, this, bulkOperation);
        this.client.performRequestAsync("POST", "/_bulk", EMPTY_MAP, entity, responseListener, ND_JSON_HEADER);
    }

    /**
     * Closes all internal resources and sets the operator as closed.
     * <p>
     * We don't close the provided {@link RestClient} instance as
     * it was provided and may be used in other classes.
     */
    @Override
    public synchronized void close() {
        // already closed
        if (this.closed) {
            return;
        }
        // flag as closed
        this.closed = true;
        // handle scheduler shutdown
        if (this.scheduler != null) {
            this.scheduledFuture.cancel(true);
            this.scheduler.shutdown();
        }
    }

    /**
     * Listening class to handle responses coming back from Elasticsearch by
     * forwarding them back through to the contained lifecycle.
     * <p>
     * This is just a container class which avoids having to bloat the definition
     * of the {@link #flush()} methods with this class implementation.
     */
    private class BulkResponseListener implements ResponseListener {

        /**
         * The internal execution identifier.
         */
        private final long id;

        /**
         * The internal operation.
         */
        private final BulkOperation operation;

        /**
         * Initializes this listener with an identifier and operation.
         *
         * @param id the execution identifier.
         * @param operation the {@link BulkOperation} being executed.
         */
        private BulkResponseListener(long id, @Nonnull BulkOperation operation) {
            this.id = id;
            this.operation = Objects.requireNonNull(operation);
        }

        /**
         * Handles request success by forwarding the response back through
         * to the lifecycle assigned to this class.
         *
         * It is important to note that this does not mean execution success,
         * but rather request success (as in HTTP).
         *
         * @param response the {@link Response} from Elasticsearch.
         */
        @Override
        public void onSuccess(final Response response) {
            execAndRelease(new Runnable() {
                @Override
                public void run() {
                    lifecycle.afterBulk(id, BulkOperator.this, operation, response);
                }
            });
        }

        /**
         * Handles request failure by forwarding the exception back through
         * to the lifecycle assigned to this class.
         *
         * @param exception the {@link Exception} thrown in execution.
         */
        @Override
        public void onFailure(final Exception exception) {
            execAndRelease(new Runnable() {
                @Override
                public void run() {
                    lifecycle.afterBulk(id, BulkOperator.this, operation, exception);
                }
            });
        }

        /**
         * Small handler to ensure that we correctly release the held
         * semaphore.
         *
         * @param block the block to execute.
         */
        private void execAndRelease(Runnable block) {
            try {
                block.run();
            } finally {
                semaphore.release();
            }
        }
    }

    /**
     * Returns a builder in order to create an operator.
     *
     * @return a new {@link Builder} instance.
     */
    public static Builder builder(RestClient client) {
        return new Builder(client);
    }

    /**
     * Builder class for a {@link BulkOperator} to provide finer control over
     * validation and mutability.
     * <p>
     * As a bulk operator is talking directly to a datasource, it makes sense
     * to have to be explicit when changing state on the operator.
     */
    public static class Builder {

        /**
         * Concurrency level of any built operators. Defaults to 1.
         */
        private int concurrency = 1;

        /**
         * A potential lifecycle listener to be notified of executions.
         */
        private BulkLifecycle lifecycle = new NoopLifecycle();

        /**
         * A potential upper bound on actions to buffer. Defaults to unbounded.
         */
        private Integer maxActions;

        /**
         * A potential interval to flush all buffered actions. Defaults to never.
         */
        private Long interval;

        /**
         * A required client to use when talking to Elasticsearch.
         */
        private RestClient client;

        /**
         * Initializes a builder instance with an Elasticsearch client.
         * <p>
         * This client must not be null, as it's required for execution.
         *
         * @param client the {@link RestClient} to execute with.
         */
        Builder(@Nonnull RestClient client) {
            this.client = Objects.requireNonNull(client);
        }

        /**
         * Modifies the concurrency associated with this builder.
         * <p>
         * The concurrency can not be set below 1, the builder
         * will enforce this lower bound.
         *
         * @param concurrency
         *      the new concurrency value.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency < 1 ? 1 : concurrency;
            return this;
        }

        /**
         * Modifies the interval associated with this builder.
         *
         * @param interval
         *      the new scheduled interval.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        public Builder interval(long interval) {
            this.interval = interval;
            return this;
        }

        /**
         * Modifies the lifecycle associated with this builder.
         * <p>
         * If you wish to unset a previously set lifecycle, you should
         * use {@link NoopLifecycle} rather than passing null.
         *
         * @param lifecycle
         *      the new lifecycle instance.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        public Builder lifecycle(@Nonnull BulkLifecycle lifecycle) {
            this.lifecycle = Objects.requireNonNull(lifecycle);
            return this;
        }

        /**
         * Modifies the max number of actions associated with this builder.
         *
         * @param maxActions
         *      the new maximum number of actions.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        public Builder maxActions(@Nullable Integer maxActions) {
            this.maxActions = maxActions;
            return this;
        }

        /**
         * Constructs a new {@link BulkOperator} from this builder.
         *
         * @return a new {@link BulkOperator} instance.
         */
        public BulkOperator build() {
            return new BulkOperator(this);
        }
    }
}
