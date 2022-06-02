package io.whitfin.elasticsearch.bulk.lifecycle;

import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.whitfin.elasticsearch.bulk.BulkOperator;

import java.util.List;

/**
 * A lifecycle implementation which retries requests in case of error.
 * Requests are simply re-queued back into the operator, to try them
 * on the next cycle.
 *
 * Be careful with this, as it will retry invalid queries; you may want
 * to use the {@link NoopLifecycle} when validating queries until you're
 * sure of format.
 *
 * This implementation mainly exists for compatibility with slower machines, such
 * as container based environments and CI environments.
 */
public class RequeueLifecycle extends NoopLifecycle {

    /**
     * Handles completion verification of a bulk execution.
     *
     * Rather than just logging the completion, this will attempt to
     * handle any errors by re-queuing them back to the same operator.
     *
     * @param executionId
     *      the bulk execution identifier.
     * @param operator
     *      the {@link BulkOperator} carrying out the request.
     * @param request
     *      the {@link BulkRequest} being executed.
     * @param response
     *      the response returned by the execution.
     */
    @Override
    public void afterBulk(long executionId, BulkOperator operator, BulkRequest request, BulkResponse response) {
        // if there are no errors, we're good to exit
        if (!response.errors()) {
            return;
        }

        // grab the items array from the bulk response
        List<BulkResponseItem> itemsArray = response.items();

        // pull back our list of actions taken in this op
        List<BulkOperation> attempt = request.operations();

        // iterate all items and check for failure
        for (int i = 0, j = itemsArray.size(); i < j; i++) {
            // if the item has a status code of anything under 400, it's success
            if (itemsArray.get(i).status() < 400) {
                continue;
            }
            // add the attempt back to the operator
            operator.add(attempt.get(i));
        }
    }
}
