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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Named;

import org.apache.metamodel.elasticsearch.ElasticSearchDataContext;
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
import org.datacleaner.beans.writers.WriteDataResult;
import org.datacleaner.beans.writers.WriteDataResultImpl;
import org.datacleaner.components.categories.WriteSuperCategory;
import org.datacleaner.components.convert.ConvertToStringTransformer;
import org.datacleaner.connection.ElasticSearchDatastore;
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
    String documentType;

    @Configured
    InputColumn<?> idColumn;

    @Configured
    boolean createIndex = false;

    @Configured
    @NumberProperty(negative = false, zero = false)
    int bulkIndexSize = 2000;

    @Configured(required = false)
    boolean automaticDateDetection = false;

    @Configured("ElasticSearch datastore")
    ElasticSearchDatastore elasticsearchDatastore;

    private ElasticSearchDataContext _dataContext;

    private AtomicInteger _counter;
    private WriteBuffer _writeBuffer;

    @Initialize
    public void init() throws IOException {

        _dataContext = (ElasticSearchDataContext) elasticsearchDatastore.openConnection().getDataContext();
        final Client client = _dataContext.getElasticSearchClient();
        _counter = new AtomicInteger(0);
        _writeBuffer = new WriteBuffer(bulkIndexSize, new ElasticSearchIndexFlushAction(elasticsearchDatastore, fields,
                documentType));

        final String indexName = elasticsearchDatastore.getIndexName();

        if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists())
            client.admin().indices().prepareCreate(indexName).execute().actionGet();

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(documentType)
                .field("date_detection", automaticDateDetection).endObject().endObject();
        client.admin().indices().preparePutMapping(indexName).setType(documentType).setSource(builder).execute()
                .actionGet();
    }

    @Close
    public void close() {
        _dataContext.getElasticSearchClient().close();
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

    public void setCreateIndex(boolean createIndex) {
        this.createIndex = createIndex;
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
