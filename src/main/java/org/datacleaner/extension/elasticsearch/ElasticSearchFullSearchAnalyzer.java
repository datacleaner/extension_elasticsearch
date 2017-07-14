package org.datacleaner.extension.elasticsearch;

import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContext;
import org.apache.metamodel.util.FileHelper;
import org.datacleaner.api.Analyzer;
import org.datacleaner.api.AnalyzerResult;
import org.datacleaner.api.Close;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.Initialize;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.TableProperty;
import org.datacleaner.api.Validate;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.UpdateableDatastoreConnection;
import org.datacleaner.connection.ElasticSearchDatastore.ClientType;
import org.datacleaner.extension.elasticsearch.ui.IllegalElasticSearchConnectorException;
import org.datacleaner.result.ListResult;
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

public class ElasticSearchFullSearchAnalyzer implements  Analyzer<ListResult<Object>>{
    
    
    public static final String PROPERTY_ES_DATASTORE = "ElasticSearch index";
    public static final String PROPERTY_DOCUMENT_TYPE = "Document type";

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
     UpdateableDatastoreConnection _connection;

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
    public void run(InputRow row, int distinctCount) {
        final Object[] result = new Object[2];

        final String input = row.getValue(searchInput);
        if (StringUtils.isNullOrEmpty(input)) {
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
            }

            final SearchHit hit = hits.getAt(0);
            result[0] = hit.getId();
            result[1] = hit.sourceAsMap();
        }
        finally{
            
        }
    
        

        
    }

    @Override
    public ListResult<Object> getResult() {
        // TODO Auto-generated method stub
        return null;
    }

}
