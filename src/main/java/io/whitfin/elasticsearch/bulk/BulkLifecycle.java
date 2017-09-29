package io.whitfin.elasticsearch.bulk;

import org.elasticsearch.client.Response;

/**
 * Lifecycle interface for bulk execution, providing the ability to
 * hook into various stages of bulk execution.
 *
 * This interface is required as executions happen in a separate thread
 * so there's no way to implement your own logging. The same execution
 * can be correlated via the `executionId` passed as first argument into
 * all callback functions in this interface.
 */
public interface BulkLifecycle {

    /**
     * Executes prior to the bulk request being forwarded to Elasticsearch.
     *
     * @param executionId
     *      the bulk execution identifier.
     * @param operator
     *      the {@link BulkOperator} carrying out the request.
     * @param bulkOperation
     *      the {@link BulkOperation} being executed.
     */
    void beforeBulk(long executionId, BulkOperator operator, BulkOperation bulkOperation);

    /**
     * Executes after a bulk execution.
     *
     * Note that this only means a successful request, not necessarily
     * a successful bulk execution.
     *
     * @param executionId
     *      the bulk execution identifier.
     * @param operator
     *      the {@link BulkOperator} carrying out the request.
     * @param bulkOperation
     *      the {@link BulkOperation} being executed.
     * @param response
     *      the response returned by the execution.
     */
    void afterBulk(long executionId, BulkOperator operator, BulkOperation bulkOperation, Response response);

    /**
     * Executes after a failed bulk execution.
     *]
     * @param executionId
     *      the bulk execution identifier.
     * @param operator
     *      the {@link BulkOperator} carrying out the request.
     * @param bulkOperation
     *      the {@link BulkOperation} being executed.
     * @param failure
     *      the {@link Throwable} caught during execution.
     */
    void afterBulk(long executionId, BulkOperator operator, BulkOperation bulkOperation, Throwable failure);
}
