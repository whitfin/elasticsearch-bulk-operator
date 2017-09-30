package io.whitfin.elasticsearch.bulk;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.immutables.value.Value;

import java.util.List;

/**
 * Definition class of a bulk operation per the Elasticsearch Bulk API.
 *
 * This class allows aggregation of {@link BulkAction} instances into
 * a single batch to be executed against Elasticsearch at a single point
 * in time, via a single HTTP request.
 *
 * The only modifiable state of this class is the actions list, which can
 * be modified via the {@link ImmutableBulkAction.Builder}. All other
 * values are derived at build time as they'll never change, and we can
 * assume that they'll always be used.
 */
@Value.Immutable
@Value.Style(depluralize = true)
@JsonSerialize(as = ImmutableBulkOperation.class)
@JsonDeserialize(as = ImmutableBulkOperation.class)
public abstract class BulkOperation {

    /**
     * The list of actions associated with this operation.
     *
     * @return a {@link List} of actions to execute.
     */
    public abstract List<BulkAction> actions();

    /**
     * The estimated size of this operation.
     *
     * @return the length of the internal payload.
     */
    @Value.Derived
    public long estimatedSizeInBytes() {
        return payload().length();
    }

    /**
     * The number of actions in this operation.
     *
     * @return the size of the internal action list.
     */
    @Value.Derived
    public long numberOfActions() {
        return actions().size();
    }

    /**
     * The textual representation of this operation.
     *
     * @return a String payload to pass to the API.
     */
    @Value.Derived
    public String payload() {
        StringBuilder builder = new StringBuilder();

        for (BulkAction action : actions()) {
            ObjectNode contentBuilder = JsonNodeFactory.instance.objectNode();
            ObjectNode operationBuilder = contentBuilder.with(action.operation());

            if (action.index() != null) {
                operationBuilder.put("_index", action.index());
            }

            if (action.type() != null) {
                operationBuilder.put("_type", action.type());
            }

            if (action.id() != null) {
                operationBuilder.put("_id", action.id());
            }

            if (action.parent() != null) {
                operationBuilder.put("_parent", action.parent());
            }

            if (action.routing() != null) {
                operationBuilder.put("_routing", action.routing());
            }

            if (action.version() != null) {
                operationBuilder.put("_version", action.version());
            }

            if (action.refresh() != null) {
                operationBuilder.put("refresh", action.refresh());
            }

            if (action.waitForActiveShards() != null) {
                operationBuilder.put("wait_for_active_shards", action.waitForActiveShards());
            }

            builder.append(contentBuilder.toString());
            builder.append("\n");
            builder.append(action.source());
            builder.append("\n");
        }

        return builder.toString();
    }

    /**
     * Returns a builder in order to create an operation.
     *
     * @return a new {@link BulkAction.Builder} instance.
     */
    @SuppressWarnings("WeakerAccess")
    public static Builder builder() {
        return new BulkOperation.Builder();
    }

    /**
     * Builder bindings to allow for creating operations with validation.
     */
    @SuppressWarnings("WeakerAccess")
    public static class Builder extends ImmutableBulkOperation.Builder { }
}
