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
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

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
 * @see <a href="http://ow.ly/VQIo30fxqly">Elasticsearch Bulk API</a>
 */
@SuppressWarnings("immutables")
@Value.Immutable(copy = false)
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
public abstract class BulkOperator implements Closeable {

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
     * Our scheduling service to allow flushing on interval.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Our future to store references to scheduled executions.
     */
    private ScheduledFuture scheduledFuture;

    /**
     * Our concurrency controller to limit access.
     */
    private Semaphore mutex;

    /**
     * The client instance for Elasticsearch communication.
     *
     * @return a configured {@link RestClient} instance.
     */
    public abstract RestClient client();

    /**
     * The concurrency level for this operator instance.
     *
     * @return a number of concurrent flushes.
     */
    @Value.Default
    public int concurrency() {
        return 1;
    }

    /**
     * The lifecycle hooks being fired by this operator.
     *
     * @return a {@link BulkLifecycle} instance.
     */
    @Value.Default
    public BulkLifecycle lifecycle() {
        return new NoopLifecycle();
    }

    /**
     * The interval on which this operator will flush.
     *
     * @return a flush interval in milliseconds.
     */
    @Nullable
    public abstract Integer interval();

    /**
     * The maximum number of actions to buffer before flushing.
     *
     * @return an maximum number of actions to buffer.
     */
    @Nullable
    public abstract Integer maxActions();

    /**
     * Validation hook to configure any requirements for properties
     * set inside the builder.
     *
     * Currently this just ensures that concurrency must be at least
     * 1, to avoid blocking behaviour.
     *
     * @return a validated {@link BulkOperator} instance.
     */
    @Value.Check
    BulkOperator validate() {
        BulkOperator initializedOperator;
        if (concurrency() > 0) {
            initializedOperator = this;
        } else {
            initializedOperator = ImmutableBulkOperator
                    .builder()
                        .from(this)
                        .concurrency(1)
                    .build();
        }
        return initializedOperator.init();
    }

    /**
     * Initializes the operator to begin receiving actions.
     * <p>
     * This effectively acts as a constructor due to the use of the
     * immutable library, allowing this operator to use certain
     * mutable state to track operations throughout.
     *
     * @return the same {@link BulkOperator} for chaining.
     */
    private synchronized BulkOperator init() {
        // set initial states
        this.current = 0;
        this.closed = false;

        // create our semaphore to control our concurrency
        this.mutex = new Semaphore(concurrency());

        // begin our bulk operation creation via builder
        this.operation = BulkOperation.builder();

        // done if there's no interval
        Integer interval = interval();
        if (interval == null) {
            return this;
        }

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
            interval,
            interval,
            TimeUnit.MILLISECONDS
        );

        // chaining
        return this;
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
        Integer maxActions = maxActions();
        if (maxActions != null && this.current >= maxActions) {
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
            this.operation.actions(Collections.<BulkAction>emptyList());
        }

        try {
            // throttle concurrency
            this.mutex.acquire();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // create a new identifier for this execution
        long bulkId = IDENTIFIERS.incrementAndGet();

        // create request listeners and convert the payload to a HttpEntity as JSON
        ResponseListener responseListener = new BulkResponseListener(bulkId, bulkOperation);
        HttpEntity entity = new NStringEntity(bulkOperation.payload(), ContentType.APPLICATION_JSON);

        // notify before listener and begin async request
        this.lifecycle().beforeBulk(bulkId, this, bulkOperation);
        this.client().performRequestAsync("POST", "/_bulk", EMPTY_MAP, entity, responseListener, ND_JSON_HEADER);
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
            this.scheduledFuture = null;
            this.scheduler.shutdown();
            this.scheduler = null;
        }
    }

    /**
     * Returns a builder in order to create an operator.
     *
     * @param client
     *      the {@link RestClient} to use to talk to Elasticsearch.
     * @return
     *      a new {@link Builder} instance from the provided client.
     */
    public static Builder builder(@Nonnull RestClient client) {
        return ImmutableBulkOperator.builder().client(client);
    }

    /**
     * Builder interface for all immutable implementations to mask the
     * use of the generated sources to avoid confusion.
     *
     * These methods are the only ones exposed from the builder to the
     * outside world (rather than just the package).
     */
    public interface Builder {

        /**
         * Initializes a builder instance with an Elasticsearch client.
         * <p>
         * This client must not be null, as it's required for execution.
         *
         * @param client the {@link RestClient} to execute with.
         */
        Builder client(RestClient client);

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
        Builder concurrency(int concurrency);

        /**
         * Modifies the interval associated with this builder.
         *
         * @param interval
         *      the new scheduled interval.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder interval(Integer interval);

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
        Builder lifecycle(BulkLifecycle lifecycle);

        /**
         * Modifies the max number of actions associated with this builder.
         *
         * @param maxActions
         *      the new maximum number of actions.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder maxActions(Integer maxActions);

        /**
         * Constructs a new {@link BulkOperator} from this builder.
         *
         * @return a new {@link BulkOperator} instance.
         */
        BulkOperator build();
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
         * <p>
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
                    lifecycle().afterBulk(id, BulkOperator.this, operation, response);
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
                    lifecycle().afterBulk(id, BulkOperator.this, operation, exception);
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
                mutex.release();
            }
        }
    }
}
