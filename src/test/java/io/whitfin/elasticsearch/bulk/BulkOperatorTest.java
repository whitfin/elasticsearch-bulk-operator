package io.whitfin.elasticsearch.bulk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.whitfin.elasticsearch.bulk.lifecycle.RequeueLifecycle;
import org.apache.http.HttpHost;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class BulkOperatorTest {

    /**
     * List of operators to clean up after completion.
     */
    private List<BulkOperator> operators;

    /**
     * List of indices to clean up after completion.
     */
    private List<String> indices;

    /**
     * Internal mapper for all JSON conversion.
     */
    private ObjectMapper mapper;

    /**
     * The internal client for Elasticsearch communication.
     */
    private RestClient restClient;

    /**
     * Creates all needed state for the internal test cases.
     */
    @BeforeClass
    public void setupClient() {
        this.mapper = new ObjectMapper();
        this.indices = new ArrayList<>();
        this.operators = new ArrayList<>();
        this.restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
    }

    /**
     * Tests writing documents on an interval.
     */
    @Test
    public void testIndexingABunchOfDocumentsOnInterval() throws Exception {
        // generate temporary resources for current test structures
        String testIndex = generateTempIndex(() -> generateRandomHexToken(6));
        BulkOperator operator = generateTempOperator(builder -> {
            // apply concurrency, interval and max actions
            return builder.concurrency(1).interval(3_000).lifecycle(new RequeueLifecycle());
        });

        // write documents and then validate existence
        writeDocumentsIntoElasticsearch(operator, testIndex, 5_000);
        validateDocumentsExist(testIndex, 5_000, Duration.TEN_SECONDS);
    }

    /**
     * Tests writing documents on a limit.
     */
    @Test
    public void testIndexingABunchOfDocumentsOnLimit() throws Exception {
        // generate temporary resources for current test structures
        String testIndex = generateTempIndex(() -> generateRandomHexToken(6));
        BulkOperator operator = generateTempOperator(builder -> {
            // apply concurrency, lifecycle and max actions
            return builder.concurrency(1).lifecycle(new RequeueLifecycle()).maxActions(1_000);
        });

        // write documents and then validate existence
        writeDocumentsIntoElasticsearch(operator, testIndex, 1_000);
        validateDocumentsExist(testIndex, 1_000, Duration.FIVE_SECONDS);
    }

    /**
     * Tests writing documents on a manual flush.
     */
    @Test
    public void testIndexingABunchOfDocumentsOnFlush() throws Exception {
        // generate temporary resources for current test structures
        String testIndex = generateTempIndex(() -> generateRandomHexToken(6));
        BulkOperator operator = generateTempOperator(builder -> {
            // apply concurrency and lifecycle
            return builder.concurrency(1).lifecycle(new RequeueLifecycle());
        });

        // write documents and then validate existence
        writeDocumentsIntoElasticsearch(operator, testIndex, 500);
        operator.flush();
        validateDocumentsExist(testIndex, 500, Duration.FIVE_SECONDS);
    }

    /**
     * Cleans up indices and operators after all tests have
     * finished executing inside this class.
     */
    @AfterClass
    public void cleanup() throws Exception {
        for (BulkOperator operator : this.operators) {
            operator.close();
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> it = this.indices.iterator();

        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(",");
            }
        }

        try {
            this.restClient.performRequest("DELETE", "/" + sb.toString());
        } catch(Exception e) {
            // never mind
        }

        this.indices = null;
        this.operators = null;
    }

    /**
     * Generates a random document, with just a key set to "value"
     * and a randomized hex token as the value.
     *
     * @return a String JSON payload.
     */
    private String generateRandomDocument() {
        return "{\"value\":\"" + generateRandomHexToken(8) + "\"}";
    }

    /**
     * Generates a random hex token of the given length.
     *
     * @param length
     *      the desired length of the generated token.
     * @return
     *      a String generated hex token.
     */
    private String generateRandomHexToken(int length) {
        SecureRandom secureRandom = new SecureRandom();
        byte[] token = new byte[length];
        secureRandom.nextBytes(token);
        return new BigInteger(1, token).toString(16);
    }

    /**
     * Generates a temporary index.
     *
     * This just schedules the index for cleanup after
     * the class has finished executing.
     *
     * @param stringSupplier
     *      the index name generator.
     * @return
     *      a String index name.
     */
    private String generateTempIndex(Supplier<String> stringSupplier) throws Exception {
        String index = stringSupplier.get();
        this.indices.add(index);
        return index;
    }

    /**
     * Generates a temporary operator.
     *
     * @param unaryOperator
     *      the operator generator.
     * @return
     *      a new operator instance
     */
    private BulkOperator generateTempOperator(UnaryOperator<BulkOperator.Builder> unaryOperator) throws Exception {
        BulkOperator operator = unaryOperator.apply(BulkOperator.builder(this.restClient)).build();
        this.operators.add(operator);
        return operator;
    }

    /**
     * Validates documents exist in Elasticsearch, waiting up
     * to the provided duration before failing.
     *
     * @param index
     *      the index to look inside for the documents.
     * @param count
     *      the number of documents expected in the index.
     * @param duration
     *      the maximum time to wait before failing.
     */
    private void validateDocumentsExist(String index, long count, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> {
            // response container
            Response response;

            try {
                // try execute the request, fail on any errors in the query
                response = restClient.performRequest("GET", "/" + index + "/_count");
            } catch(Exception e) {
                return false;
            }

            // if the request failed for some reason, return false
            if (response.getStatusLine().getStatusCode() != 200) {
                return false;
            }

            // validate the body has a count matching the expected, or false
            JsonNode body = mapper.readTree(response.getEntity().getContent());
            return body.path("count").asLong(-1) == count;
        });
    }

    /**
     * Writes a number of documents into Elasticsearch via
     * the provided operator.
     *
     * @param operator
     *      the operator instance to use when writing.
     * @param index
     *      the index to write documents into.
     * @param count
     *      the number of documents to write.
     */
    private void writeDocumentsIntoElasticsearch(BulkOperator operator, String index, long count) {
        for (int i = 0; i < count; i++) {
            BulkAction action = BulkAction
                    .builder()
                        .operation("index")
                        .index(index)
                        .type("test")
                        .source(generateRandomDocument())
                    .build();

            operator.add(action);
        }
    }
}
