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
package org.eobjects.datacleaner.extension.elasticsearch;

import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Named;

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
import org.datacleaner.util.WriteBuffer;
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

    @Configured(required = false)
    String[] clusterHosts = new String[0];

    @Configured
    String clusterName = "elasticsearch";

    @Configured
    String indexName;

    @Configured
    String documentType;

    @Configured
    InputColumn<?> idColumn;

    @Configured
    boolean createIndex = false;

    @Configured
    @NumberProperty(negative = false, zero = false)
    int bulkIndexSize = 2000;

    private ElasticSearchClientFactory _clientFactory;
    private AtomicInteger _counter;
    private WriteBuffer _writeBuffer;

    @Initialize
    public void init() {
        _clientFactory = new ElasticSearchClientFactory(clusterHosts, clusterName);

        _counter = new AtomicInteger(0);
        _writeBuffer = new WriteBuffer(bulkIndexSize, new ElasticSearchIndexFlushAction(_clientFactory, fields,
                indexName, documentType));
    }

    @Close
    public void close() {
        _clientFactory.close();
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

    public void setClusterHosts(String[] clusterHosts) {
        this.clusterHosts = clusterHosts;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setValues(InputColumn<?>[] values) {
        this.values = values;
    }
}
