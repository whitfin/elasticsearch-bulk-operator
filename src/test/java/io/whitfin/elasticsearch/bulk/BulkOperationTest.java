package io.whitfin.elasticsearch.bulk;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BulkOperationTest {

    /**
     * Tests _bulk payload generation from an action.
     */
    @Test
    public void testPayloadGeneration() {
        BulkAction action = BulkAction
                .builder()
                    .operation("index")
                    .index("test_index")
                    .type("test_type")
                    .id("test_id")
                    .parent("test_parent")
                    .routing("test_routing")
                    .version(1)
                    .refresh(true)
                    .source("{\"test_key\":\"test_value\"}")
                    .waitForActiveShards(true)
                .build();

        BulkOperation operation = BulkOperation
                .builder()
                    .addAction(action)
                .build();

        Assert.assertEquals(operation.numberOfActions(), 1);
        Assert.assertEquals(operation.actions().get(0), action);

        String expectedPayload = "{\"index\":{\"_index\":\"test_index\",\"_type\":\"test_type\",\"_id\":\"test_id\"," +
                "\"_parent\":\"test_parent\",\"_routing\":\"test_routing\",\"_version\":1,\"refresh\":true," +
                "\"wait_for_active_shards\":true}}\n{\"test_key\":\"test_value\"}\n";

        Assert.assertEquals(operation.payload(), expectedPayload);
        Assert.assertEquals(operation.estimatedSizeInBytes(), expectedPayload.length());
    }
}
