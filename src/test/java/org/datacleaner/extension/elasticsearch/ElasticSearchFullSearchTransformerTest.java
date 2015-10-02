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
import java.util.TreeMap;

import junit.framework.TestCase;

import org.datacleaner.api.InputColumn;
import org.datacleaner.api.OutputColumns;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.ElasticSearchDatastore.ClientType;
import org.datacleaner.data.MockInputColumn;
import org.datacleaner.data.MockInputRow;
import org.datacleaner.extension.elasticsearch.ElasticSearchFullSearchTransformer;
import org.elasticsearch.common.collect.MapBuilder;

public class ElasticSearchFullSearchTransformerTest extends TestCase {

    private ElasticSearchTestServer _server;
    private ElasticSearchDatastore _elasticSearchDatastore;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        _server = new ElasticSearchTestServer();
        _server.startup();
        _elasticSearchDatastore = new ElasticSearchDatastore(ElasticSearchTestServer.DATASTORE_NAME,
                ClientType.TRANSPORT, "localhost", Integer.parseInt(ElasticSearchTestServer.TRANSPORT_PORT),
                ElasticSearchTestServer.CLUSTER_NAME, ElasticSearchTestServer.INDEX_NAME);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        _server.close();
    }

    public void testTransform() throws Exception {
        final InputColumn<String> col1 = new MockInputColumn<String>("col1");

        final ElasticSearchFullSearchTransformer transformer = new ElasticSearchFullSearchTransformer();
        transformer.searchInput = col1;
        transformer.documentType = ElasticSearchTestServer.DOCUMENT_TYPE;
        transformer.elasticsearchDatastore = _elasticSearchDatastore;

        OutputColumns out = transformer.getOutputColumns();
        assertEquals("OutputColumns[Document ID, Document]", out.toString());

        try {

            _server.truncateIndex();
            assertEquals(0, _server.getDocumentCount());

            _server.addDocument("cph", MapBuilder.newMapBuilder().put("city", "Copenhagen").put("country", "Denmark")
                    .map());
            _server.addDocument("ams", MapBuilder.newMapBuilder().put("city", "Amsterdam")
                    .put("country", "Netherlands").map());
            _server.addDocument("del", MapBuilder.newMapBuilder().put("city", "Delhi").put("country", "India").map());

            Object[] output;

            output = transformer.transform(new MockInputRow().put(col1, "Copenhagen"));
            assertEquals("cph", String.valueOf(output[0]));

            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) output[1];
            assertNotNull(map);
            assertEquals("{city=Copenhagen, country=Denmark}", new TreeMap<>(map).toString());

            output = transformer.transform(new MockInputRow().put(col1, "n/a"));
            assertEquals("null", String.valueOf(output[0]));
            assertEquals("null", String.valueOf(output[1]));

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
