package io.whitfin.elasticsearch.bulk.lifecycle;

import io.whitfin.elasticsearch.bulk.BulkLifecycle;
import io.whitfin.elasticsearch.bulk.BulkOperation;
import io.whitfin.elasticsearch.bulk.BulkOperator;
import org.elasticsearch.client.Response;

/**
 * A no-op lifecycle to act as a null lifecycle.
 *
 * This lifecycle can be extended to avoid having to define
 * the bindings for methods you do not wish to implement.
 */
public class NoopLifecycle implements BulkLifecycle {

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeBulk(long executionId, BulkOperator operator, BulkOperation bulkOperation) {
        // executed before the bulk request is sent
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterBulk(long executionId, BulkOperator operator, BulkOperation bulkOperation, Response response) {
        // executed after a successful bulk request
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterBulk(long executionId, BulkOperator operator, BulkOperation bulkOperation, Throwable failure) {
        // executed after a failed bulk request
    }
}
