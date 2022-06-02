# Elasticsearch Bulk Operator (REST)
[![Build Status](https://img.shields.io/github/workflow/status/whitfin/elasticsearch-bulk-operator/CI)](https://github.com/whitfin/elasticsearch-bulk-operator/actions) [![Maven Central](https://img.shields.io/maven-central/v/io.whitfin/elasticsearch-bulk-operator.svg)]() [![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://javadoc.io/doc/io.whitfin/elasticsearch-bulk-operator)

**Update 2022**: Elasticsearch has once again replaced their core library, this time with a new [Java API](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html).
Similar to the last migration, this omits utility around the `BulkProcessor` and so I will be updating this library in
the meantime. You can track the official addition [here](https://github.com/elastic/elasticsearch-java/issues/108). This
time around most of the core ES classes are reusable, which makes it a much smaller effort. If you need the 7.x version
of this library, please use the 1.x releases.

**Update 2021**: As of the most recent Elasticsearch versions, there is once again an [official implementation](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high-document-bulk.html)
of `BulkProcessor` shipped in the latest releases. I'd encourage users to update and migrate over.

This repo contains an implementation of something similar to the `BulkProcessor` included in Elasticsearch 2.x. The intent
is to make it easier to carry out bulk actions against Elasticsearch using just the REST client which doesn't yet include
an easy way to carry out `_bulk` requests.

This implementation has been in use in production at scale (roughly 2000 documents per second) for approximately 6 months
without issue at the time of writing (September 2017). If you do find any issues, please file an issue (or a PR!) and I'll
try to fix it up as soon as possible.

### Installation

Just add the dependency as usual (you might have to check for the latest version, rather than what's shown below):

```xml
<dependency>
    <groupId>io.whitfin</groupId>
    <artifactId>elasticsearch-bulk-operator</artifactId>
    <version>2.0.0</version>
</dependency>
```

### Usage

The interface is deliberately small; you will only interact with a couple of classes. A `BulkOperator` will carry out
requests periodically, based on rules you provide during construction (see the docs for info on what you can set). An
operator will queue many `BulkOperation` instances into a single bulk request to Elasticsearch.

```java
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import io.whitfin.elasticsearch.bulk.BulkOperator;
import io.whitfin.elasticsearch.bulk.lifecycle.RequeueLifecycle;
    
public class BulkExample {

    public static void main(String[] args) {
        // create your ES Java client somehow
        ElasticsearchClient client = createClient();

        // create an operator to flush each minute
        BulkOperator operator = BulkOperator
                .builder(client)
                    .concurrency(3)
                    .interval(60_000)
                    .lifecycle(new RequeueLifecycle())
                    .maxActions(10_000)
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("test", true);

        // create an index action (or whatever else you want)
        IndexOperation<Map<String, Object>> index = new IndexOperation
                .Builder<Map<String, Object>>()
                    .index("my_test_index")
                    .document(document)
                .build();

        // create a bulk operation from the action we have
        BulkOperation action = new BulkOperation.Builder()
                .index(index)
                .build();

        // queue it up!
        operator.add(action);
    }
}
```

For any other functionality, please see the documentation or the code itself.

### Flush Options

There are several options which can be applied to an operator to control how flushing occurs;

1. You can define `maxActions` on an operator to provide a limit on the buffer stored internally before flushing.
2. You can define `interval` on an operator to provide a schedule (in millis) on which to flush.
3. You can opt (default) to manually flush by calling `flush()` on an operator.
4. You can do any of the above in any combination to work with multiple flush triggers.

### Lifecycles

You can attach `BulkLifecycle` instances to your operator to hook into various stages of the operator lifecycle.
There are currently only two implementations shipped; `NoopImplementation` (which does nothing) and `RequeueLifecycle`
which will add failed requests back to the operator (for version conflicts, etc). You can easily create your own by using
the interface class and registering it on your operator:

```java
public class CustomLifecycle implements BulkLifecycle {

    @Override
    public void beforeBulk(long executionId, BulkOperator operator, BulkRequest request) {
        // executed before the batch is sent
    }

    @Override
    public void afterBulk(long executionId, BulkOperator operator, BulkRequest request, BulkResponse response) {
        // executed after a success batch request
    }

    @Override
    public void afterBulk(long executionId, BulkOperator operator, BulkRequest request, Throwable failure) {
        // executed after a failed batch request
    }
}
```
