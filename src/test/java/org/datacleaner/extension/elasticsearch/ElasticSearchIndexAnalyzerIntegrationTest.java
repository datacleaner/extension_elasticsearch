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

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.datacleaner.beans.writers.WriteDataResult;
import org.datacleaner.components.maxrows.MaxRowsFilter;
import org.datacleaner.configuration.DataCleanerConfiguration;
import org.datacleaner.configuration.DataCleanerConfigurationImpl;
import org.datacleaner.configuration.DataCleanerEnvironment;
import org.datacleaner.configuration.DataCleanerEnvironmentImpl;
import org.datacleaner.connection.CsvDatastore;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.ElasticSearchDatastore.ClientType;
import org.datacleaner.descriptors.Descriptors;
import org.datacleaner.descriptors.SimpleDescriptorProvider;
import org.datacleaner.job.AnalysisJob;
import org.datacleaner.job.JaxbJobReader;
import org.datacleaner.job.concurrent.MultiThreadedTaskRunner;
import org.datacleaner.job.concurrent.TaskRunner;
import org.datacleaner.job.runner.AnalysisResultFuture;
import org.datacleaner.job.runner.AnalysisRunnerImpl;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

public class ElasticSearchIndexAnalyzerIntegrationTest extends TestCase {

    private ElasticSearchTestServer _server;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        _server = new ElasticSearchTestServer();
        _server.startup();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        _server.close();
    }

    public void testIndex() throws Throwable {
        final Resource resource = new FileResource("src/test/resources/AddressAccess.csv");
        final String filename = "AddressAccess.csv";
        final CsvDatastore ds = new CsvDatastore("AddressAccess.csv", resource, filename, '"', ';', '\\', "UTF8", true,
                1);
        final ElasticSearchDatastore elasticSearchDatastore = new ElasticSearchDatastore(
                ElasticSearchTestServer.DATASTORE_NAME, ClientType.TRANSPORT, "localhost",
                Integer.parseInt(ElasticSearchTestServer.TRANSPORT_PORT), ElasticSearchTestServer.CLUSTER_NAME,
                ElasticSearchTestServer.INDEX_NAME);
        final SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider();
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors.ofAnalyzer(ElasticSearchIndexAnalyzer.class));
        descriptorProvider.addFilterBeanDescriptor(Descriptors.ofFilter(MaxRowsFilter.class));

        final TaskRunner taskRunner = new MultiThreadedTaskRunner(10);
        final DataCleanerEnvironment environment = new DataCleanerEnvironmentImpl().withTaskRunner(taskRunner)
                .withDescriptorProvider(descriptorProvider);
        final DataCleanerConfiguration conf = new DataCleanerConfigurationImpl().withEnvironment(environment)
                .withDatastores(ds, elasticSearchDatastore);

        final AnalysisJob job = new JaxbJobReader(conf).create(new File("src/test/resources/es_test.analysis.xml"))
                .toAnalysisJob();

        _server.truncateIndex();

        final AnalysisResultFuture resultFuture = new AnalysisRunnerImpl(conf).run(job);

        resultFuture.await();

        if (resultFuture.isErrornous()) {
            List<Throwable> errors = resultFuture.getErrors();
            for (Throwable error : errors) {
                error.printStackTrace();
            }
            throw errors.get(0);
        }

        WriteDataResult result = (WriteDataResult) resultFuture.getResults().get(0);
        assertEquals(9, result.getWrittenRowCount());

        assertEquals(9, _server.getDocumentCount());

        try (Client client = _server.getClient()) {
            SearchResponse searchResponse = new SearchRequestBuilder(client)
                    .setIndices(ElasticSearchTestServer.INDEX_NAME).setTypes(ElasticSearchTestServer.DOCUMENT_TYPE)
                    .setQuery(QueryBuilders.queryString("Allersgade")).execute().actionGet();
            assertEquals(2l, searchResponse.getHits().getTotalHits());

            searchResponse = new SearchRequestBuilder(client)
                    .setIndices(ElasticSearchTestServer.INDEX_NAME).setTypes(ElasticSearchTestServer.DOCUMENT_TYPE)
                    .setQuery(QueryBuilders.termQuery("street.raw", "Burgmeester Daleslaan")).execute().actionGet();
            assertEquals(1l, searchResponse.getHits().getTotalHits());

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
