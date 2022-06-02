package io.whitfin.elasticsearch.bulk;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import io.whitfin.elasticsearch.bulk.lifecycle.NoopLifecycle;
import org.elasticsearch.client.RestClient;
import org.immutables.value.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

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
@Value.Style(depluralize = true, jdkOnly = true, visibility = PACKAGE)
public abstract class BulkOperator implements Closeable {

    /**
     * Internal counter to use to generate execution identifiers.
     */
    private static final AtomicLong IDENTIFIERS = new AtomicLong();

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
    private BulkRequest.Builder request;

    /**
     * Our scheduling service to allow flushing on interval.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Our future to store references to scheduled executions.
     */
    private ScheduledFuture<?> scheduledFuture;

    /**
     * Our concurrency controller to limit access.
     */
    private Semaphore mutex;

    /**
     * The client instance for Elasticsearch communication.
     *
     * @return a configured {@link ElasticsearchClient} instance.
     */
    public abstract ElasticsearchClient client();

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
        this.request = new BulkRequest.Builder();

        // done if there's no interval
        Integer interval = interval();
        if (interval == null) {
            return this;
        }

        // set up the new scheduled pool with a single threads
        this.scheduler = Executors.newScheduledThreadPool(1);

        // schedule our flush to happen on the provided interval
        this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
            this::flush,
            interval,
            interval,
            TimeUnit.MILLISECONDS
        );

        // chaining
        return this;
    }

    /**
     * Adds new {@link BulkOperation} instances to the internal
     * bulk operation builder.
     *
     * @param bulkActions
     *      the {@link BulkOperation} instances to add.
     * @return
     *      the {@link BulkOperator} instance for chaining.
     */
    public synchronized BulkOperator add(BulkOperation... bulkActions) {
        return this.add(Arrays.asList(bulkActions));
    }

    /**
     * Adds new {@link BulkOperation} instances to the internal
     * bulk operation builder.
     *
     * @param bulkActions
     *      the {@link BulkOperation} instances to add.
     * @return
     *      the {@link BulkOperator} instance for chaining.
     */
    public synchronized BulkOperator add(List<BulkOperation> bulkActions) {
        // short-circuit if closed
        if (this.closed) {
            throw new IllegalStateException("BulkOperator already closed");
        }

        // add the action
        this.current++;
        this.request.operations(bulkActions);

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
        BulkRequest bulkRequest;
        synchronized (BulkOperator.this) {
            bulkRequest = this.request.refresh(Refresh.True).build();
            this.current = 0;
            this.request = new BulkRequest.Builder();
        }

        try {
            // throttle concurrency
            this.mutex.acquire();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // create a new identifier for this execution
        long bulkId = IDENTIFIERS.incrementAndGet();

        // notify before listener and begin async request
        this.lifecycle().beforeBulk(bulkId, this, bulkRequest);

        try {
            // handle execution and handling of a successful bulk request
            this.lifecycle().afterBulk(bulkId, this, bulkRequest, this.client().bulk(bulkRequest));
        } catch (Exception e) {
            // handle exceptions thrown during a bulk request
            this.lifecycle().afterBulk(bulkId, this, bulkRequest, e);
        } finally {
            // release our lock
            this.mutex.release();
        }
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
     *      the {@link ElasticsearchClient} to use to talk to Elasticsearch.
     * @return
     *      a new {@link Builder} instance from the provided client.
     */
    public static Builder builder(@Nonnull ElasticsearchClient client) {
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
         * @param client
         *      the {@link ElasticsearchClient} to execute with.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder client(ElasticsearchClient client);

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
}
