/**
 * DataCleaner (community edition)
 * Copyright (C) 2013 Human Inference
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.datacleaner.extension.elasticsearch;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchTestServer {

    public static final String DATASTORE_NAME = "testdatastore";
    public static final String HTTP_PORT = "9242";
    public static final String INDEX_NAME = "testindex";
    public static final String CLUSTER_NAME = "testcluster";
    public static final String TRANSPORT_PORT = "9342";
    public static final String DOCUMENT_TYPE = "testdoc";

    public static void main(String[] args) throws Exception {
        ElasticSearchTestServer server = new ElasticSearchTestServer();
        server.startup();

        Thread.sleep(60 * 1000);

        server.close();
    }

    private Node _node;

    public ElasticSearchTestServer() {
    }

    public void startup() throws Exception {
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        settings.put("node.name", "testnode");
        settings.put("gateway.type", "none");
        settings.put("path.data", "target/search-data");
        settings.put("http.enabled", true);

        settings.put("http.port", HTTP_PORT);
        // settings.put("index.compound_format", false);
        settings.put("transport.tcp.port", TRANSPORT_PORT);
        _node = NodeBuilder.nodeBuilder().settings(settings).clusterName(CLUSTER_NAME).data(true).local(false).node();

        try (Client client = _node.client()) {
            IndicesAdminClient indicesAdmin = client.admin().indices();
            if (!indicesAdmin.exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists()) {
                indicesAdmin.create(new CreateIndexRequest(INDEX_NAME)).actionGet();
            }
        }

        System.out.println("--- ElasticSearchTestServer started ---");
    }

    public Client getClient() {
        return _node.client();
    }

    public IndexDeleteByQueryResponse truncateIndex() throws InterruptedException, ExecutionException {
        try (Client client = getClient()) {
            QueryBuilder queryBuilder = new MatchAllQueryBuilder();
            ListenableActionFuture<DeleteByQueryResponse> response = client.prepareDeleteByQuery(INDEX_NAME)
                    .setTypes(DOCUMENT_TYPE).setQuery(queryBuilder).execute();
            DeleteByQueryResponse deleteByQueryResponse = response.get();
            IndexDeleteByQueryResponse indexResult = deleteByQueryResponse.getIndex(INDEX_NAME);
            return indexResult;
        }
    }

    public long getDocumentCount() throws Exception {
        try (Client client = getClient()) {
            client.admin().indices().refresh(new RefreshRequest(INDEX_NAME)).actionGet();

            ActionFuture<CountResponse> response = client.count(new CountRequest(INDEX_NAME).types(DOCUMENT_TYPE));
            CountResponse countResponse = response.get();
            return countResponse.getCount();
        }
    }

    public void close() {
        _node.client().close();
        _node.close();
        System.out.println("--- ElasticSearchTestServer closed ---" + _node.isClosed());
    }

    public void addDocument(String id, Map<?, ?> map) {
        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME, DOCUMENT_TYPE, id).source(map);
        try (Client client = getClient()) {
            client.index(indexRequest).actionGet();

            client.admin().indices().refresh(new RefreshRequest(INDEX_NAME)).actionGet();
        }
    }
}
