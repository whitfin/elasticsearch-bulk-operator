package io.whitfin.elasticsearch.bulk.lifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.whitfin.elasticsearch.bulk.BulkAction;
import io.whitfin.elasticsearch.bulk.BulkOperation;
import io.whitfin.elasticsearch.bulk.BulkOperator;
import org.elasticsearch.client.Response;

import java.io.IOException;
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
     * Static instance of an internal mapper to use when parsing responses.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
     * @param operation
     *      the {@link BulkOperation} being executed.
     * @param response
     *      the response returned by the execution.
     */
    @Override
    public void afterBulk(long executionId, BulkOperator operator, BulkOperation operation, Response response) {
        JsonNode bulkResponse;

        try {
            // parse the bulk response back as a JsonNode instance
            bulkResponse = MAPPER.readTree(response.getEntity().getContent());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // if there are no errors, we're good to exit
        if (!bulkResponse.path("errors").asBoolean()) {
            return;
        }

        // grab the items array from the bulk response
        JsonNode itemsArray = bulkResponse.path("items");

        // pull back our list of actions taken in this op
        List<BulkAction> attempt = operation.actions();

        // iterate all items and check for failure
        for (int i = 0, j = itemsArray.size(); i < j; i++) {
            // if the item has a status code of anything under 400, it's success
            if (itemsArray.get(i).path("index").path("status").asInt() < 400) {
                continue;
            }
            // add the attempt back to the operator
            operator.add(attempt.get(i));
        }
    }
}
