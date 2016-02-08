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

import javax.inject.Named;

import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContext;
import org.apache.metamodel.util.FileHelper;
import org.datacleaner.api.Categorized;
import org.datacleaner.api.Close;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.Initialize;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.OutputColumns;
import org.datacleaner.api.TableProperty;
import org.datacleaner.api.Validate;
import org.datacleaner.components.categories.ImproveSuperCategory;
import org.datacleaner.components.categories.ReferenceDataCategory;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.ElasticSearchDatastore.ClientType;
import org.datacleaner.connection.UpdateableDatastoreConnection;
import org.datacleaner.extension.elasticsearch.ui.IllegalElasticSearchConnectorException;
import org.datacleaner.util.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Named("ElasticSearch full text search")
@Description("Performs a full text search for every record into an ElasticSearch search index.")
@Categorized(superCategory = ImproveSuperCategory.class, value = ReferenceDataCategory.class)
public class ElasticSearchFullSearchTransformer implements ElasticSearchTransformer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchFullSearchTransformer.class);

    @Configured
    InputColumn<String> searchInput;

    @Configured(order = 1, value = PROPERTY_ES_DATASTORE)
    ElasticSearchDatastore elasticsearchDatastore;

    @Configured(order = 2, value = PROPERTY_DOCUMENT_TYPE)
    @TableProperty
    String documentType;

    @Configured(order = 3, required = false)
    String analyzerName;

    @Configured(order = 4, required = false)
    String searchFieldName;
    
    private UpdateableDatastoreConnection _connection;
    
    @Validate
    public void validate() {
        final ClientType clientType = elasticsearchDatastore.getClientType();
        switch (clientType) {
        case NODE:
        case TRANSPORT:
            return;
        case REST:
            throw new IllegalElasticSearchConnectorException();
        default:
            // do nothing
        }
    }
    
    @Initialize
    public void init() {
        _connection = elasticsearchDatastore.openConnection();
    }
    
    @Close
    public void close() {
        if (_connection != null) {
            FileHelper.safeClose(_connection);
            _connection = null;
        }
    }

    @Override
    public OutputColumns getOutputColumns() {
        String[] names = new String[] { "Document ID", "Document" };
        Class<?>[] types = new Class[] { String.class, Map.class };
        return new OutputColumns(names, types);
    }

    @Override
    public Object[] transform(InputRow row) {
        final Object[] result = new Object[2];

        final String input = row.getValue(searchInput);
        if (StringUtils.isNullOrEmpty(input)) {
            return result;
        }
        try {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) _connection.getDataContext();
            final Client client = dataContext.getElasticSearchClient();
            MatchQueryBuilder query;
            if (StringUtils.isNullOrEmpty(searchFieldName)) {
                query = QueryBuilders.matchQuery("_all", input);
            } else {
                query = QueryBuilders.matchQuery(searchFieldName, input);
            }

            if (!StringUtils.isNullOrEmpty(analyzerName)) {
                query = query.analyzer(analyzerName);
            }

            final SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
                    .setIndices(elasticsearchDatastore.getIndexName()).setTypes(documentType).setQuery(query)
                    .setSize(1).setSearchType(SearchType.QUERY_AND_FETCH).setExplain(true);

            final SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            if (hits.getTotalHits() == 0) {
                return result;
            }

            final SearchHit hit = hits.getAt(0);
            result[0] = hit.getId();
            result[1] = hit.sourceAsMap();

            return result;
        } catch (Exception e) {
            logger.error("Exception while running the ElasticSearchFullSearchTransformer", e);
            throw e;
        }
    }
}
