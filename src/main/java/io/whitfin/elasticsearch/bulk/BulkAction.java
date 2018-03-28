package io.whitfin.elasticsearch.bulk;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import javax.annotation.Nullable;

/**
 * Interface for a bulk action as per the Elasticsearch API specification.
 *
 * A {@link BulkAction} is an action which can be queued into a {@link BulkOperation}
 * to carry out batch actions against Elasticsearch. Each action has a contained
 * definition as the bulk does not have any knowledge of the actions themselves (e.g.
 * it doesn't infer indices, etc).
 *
 * Almost everything in here is optional, aside from the operation name itself. This
 * is because only the index and type make sense otherwise, but they can be specified
 * in the API call itself via the Elasticsearch path and so they must be nullable.
 *
 * @see <a href="http://ow.ly/VQIo30fxqly">Elasticsearch Bulk API</a>
 */
@Value.Immutable(copy = false)
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableBulkAction.class)
@JsonDeserialize(as = ImmutableBulkAction.class)
public abstract class BulkAction {

    /**
     * The type of operation to carry out inside this action.
     *
     * @return a String operation name.
     */
    public abstract String operation();

    /**
     * The index targeted by this action.
     *
     * @return a String index name.
     */
    @Nullable
    public abstract String index();

    /**
     * The index type targeted by this action.
     *
     * @return a String type name.
     */
    @Nullable
    public abstract String type();

    /**
     * The document identifier targeted by this action.
     *
     * @return a String document identifier.
     */
    @Nullable
    public abstract String id();

    /**
     * The parent identifier associated with this action.
     *
     * @return a String parent identifier.
     */
    @Nullable
    public abstract String parent();

    /**
     * The routing identifier associated with this action.
     *
     * @return a String routing identifier.
     */
    @Nullable
    public abstract String routing();

    /**
     * The source body associated with this action.
     *
     * @return a String source body.
     */
    @Nullable
    public abstract String source();

    /**
     * The document version associated with this action.
     *
     * @return an Integer document version
     */
    @Nullable
    public abstract Integer version();

    /**
     * The document version type associated with this action.
     *
     * @return a String version type
     */
    @Nullable
    public abstract String versionType();

    /**
     * Whether this action should cause a refresh or not.
     *
     * @return true if a refresh should be triggered.
     */
    @Nullable
    public abstract Boolean refresh();

    /**
     * Whether this action should wait for shards or not.
     *
     * @return true if the action should wait for shards.
     */
    @Nullable
    public abstract Boolean waitForActiveShards();

    /**
     * Returns a builder in order to create an action.
     *
     * @return a new {@link Builder} instance.
     */
    public static Builder builder() {
        return ImmutableBulkAction.builder();
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
         * Sets the operation being carried out in the action.
         *
         * @param operation
         *      the name of the Elasticsearch operation.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder operation(String operation);

        /**
         * Sets the name of the targeted index, if any.
         *
         * @param index
         *      the name of the targeted index.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder index(String index);

        /**
         * Sets the name of the targeted type, if any.
         *
         * @param type
         *      the name of the targeted type.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder type(String type);

        /**
         * Sets the document identifier, if any.
         *
         * @param id
         *      the document identifier to operate on.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder id(String id);

        /**
         * Sets the parent identifier, if any.
         *
         * @param parent
         *      the parent document identifier.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder parent(String parent);

        /**
         * Sets the routing key of the action, if any.
         *
         * @param routing
         *      the routing key to apply to the operation.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder routing(String routing);

        /**
         * Sets the source body of the action.
         *
         * @param source
         *      the source of the request as a {@link String}.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder source(String source);

        /**
         * Sets the version of the target document, if any.
         *
         * @param version
         *      an integer version identifier.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder version(Integer version);

        /**
         * Sets the version type of the target document, if any.
         *
         * @param versionType
         *      a version type identifier.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder versionType(String versionType);

        /**
         * Sets whether the action should trigger a refresh.
         *
         * @param refresh
         *      a boolean representing a triggered refresh.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder refresh(Boolean refresh);

        /**
         * Sets whether the action should wait for active shards.
         *
         * @param waitForActiveShards
         *      a boolean representing whether to wait or not.
         * @return
         *      the {@link Builder} instance for chaining calls.
         */
        Builder waitForActiveShards(Boolean waitForActiveShards);

        /**
         * Constructs a new {@link BulkAction} from this builder.
         *
         * @return a new {@link BulkAction} instance.
         */
        BulkAction build();
    }
}
