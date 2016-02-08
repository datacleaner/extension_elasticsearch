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

import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Named;

import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContext;
import org.apache.metamodel.util.FileHelper;
import org.datacleaner.api.Analyzer;
import org.datacleaner.api.Categorized;
import org.datacleaner.api.Close;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.Initialize;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.MappedProperty;
import org.datacleaner.api.NumberProperty;
import org.datacleaner.api.Validate;
import org.datacleaner.beans.writers.WriteDataResult;
import org.datacleaner.beans.writers.WriteDataResultImpl;
import org.datacleaner.components.categories.WriteSuperCategory;
import org.datacleaner.components.convert.ConvertToStringTransformer;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.UpdateableDatastoreConnection;
import org.datacleaner.connection.ElasticSearchDatastore.ClientType;
import org.datacleaner.extension.elasticsearch.ui.IllegalElasticSearchConnectorException;
import org.datacleaner.util.WriteBuffer;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Named("ElasticSearch indexer")
@Description("Consumes records and indexes them in a ElasticSearch search index.")
@Categorized(superCategory = WriteSuperCategory.class)
public class ElasticSearchIndexAnalyzer implements Analyzer<WriteDataResult> {

    public static final String PROPERTY_INPUT_COLUMNS = "Values";
    public static final String PROPERTY_FIELD_NAMES = "Fields";

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchIndexAnalyzer.class);

    @Configured(PROPERTY_INPUT_COLUMNS)
    InputColumn<?>[] values;

    @Configured(PROPERTY_FIELD_NAMES)
    @MappedProperty(PROPERTY_INPUT_COLUMNS)
    String[] fields;

    @Configured
    InputColumn<?> idColumn;

    @Configured(value = "ElasticSearch index", order = 1)
    ElasticSearchDatastore elasticsearchDatastore;

    @Configured(order = 2)
    String documentType;

    @Configured
    @NumberProperty(negative = false, zero = false)
    int bulkIndexSize = 2000;

    @Configured(required = false)
    boolean automaticDateDetection = false;

    private AtomicInteger _counter;
    private WriteBuffer _writeBuffer;
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
    public void init() throws Exception {
        _connection = elasticsearchDatastore.openConnection();

        try {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) _connection.getDataContext();

            final Client client = dataContext.getElasticSearchClient();
            _counter = new AtomicInteger(0);
            _writeBuffer = new WriteBuffer(bulkIndexSize, new ElasticSearchIndexFlushAction(dataContext, fields,
                    documentType));

            final String indexName = elasticsearchDatastore.getIndexName();

            if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists())
                client.admin().indices().prepareCreate(indexName).execute().actionGet();

            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(documentType)
                    .field("date_detection", automaticDateDetection).endObject().endObject();
            client.admin().indices().preparePutMapping(indexName).setType(documentType).setSource(builder).execute()
                    .actionGet();
        } catch (Exception e) {
            logger.error("Exception while running the ElasticSearchIndexAnalyzer", e);
            FileHelper.safeClose(_connection);
            throw e;
        }
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
        final Object[] record = new Object[values.length + 1];
        final String id = ConvertToStringTransformer.transformValue(row.getValue(idColumn));
        if (id == null) {
            logger.warn("Skipping record because ID is null: {}", row);
            return;
        }
        record[0] = id;
        for (int i = 0; i < values.length; i++) {
            Object value = row.getValue(values[i]);
            record[i + 1] = value;
        }
        _writeBuffer.addToBuffer(record);
        _counter.incrementAndGet();
    }

    @Override
    public WriteDataResult getResult() {
        _writeBuffer.flushBuffer();

        final int indexCount = _counter.get();
        final WriteDataResult result = new WriteDataResultImpl(indexCount, 0, 0);
        return result;
    }

    public void setBulkIndexSize(int bulkIndexSize) {
        this.bulkIndexSize = bulkIndexSize;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setIdColumn(InputColumn<?> idColumn) {
        this.idColumn = idColumn;
    }

    public void setValues(InputColumn<?>[] values) {
        this.values = values;
    }
}
