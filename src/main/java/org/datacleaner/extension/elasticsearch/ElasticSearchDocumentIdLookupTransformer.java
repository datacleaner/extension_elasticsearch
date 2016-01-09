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
import org.datacleaner.components.categories.ImproveSuperCategory;
import org.datacleaner.components.categories.ReferenceDataCategory;
import org.datacleaner.components.convert.ConvertToStringTransformer;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.UpdateableDatastoreConnection;
import org.datacleaner.util.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.get.GetField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Named("ElasticSearch document ID lookup")
@Description("Look up documents in ElasticSearch by providing a document ID")
@Categorized(superCategory = ImproveSuperCategory.class, value = ReferenceDataCategory.class)
public class ElasticSearchDocumentIdLookupTransformer implements ElasticSearchTransformer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDocumentIdLookupTransformer.class);

    @Configured
    InputColumn<?> documentId;

    @Configured(order = 1, value = PROPERTY_ES_DATASTORE)
    ElasticSearchDatastore elasticsearchDatastore;

    @Configured(order = 2, value = PROPERTY_DOCUMENT_TYPE)
    @TableProperty
    String documentType;

    @Configured(order = 3)
    @Description("Fields to return")
    String[] fields;

    private UpdateableDatastoreConnection _connection;

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
        return new OutputColumns(String.class, fields);
    }

    @Override
    public String[] transform(InputRow row) {

        try {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) _connection.getDataContext();

            final Client client = dataContext.getElasticSearchClient();
            final String[] result = new String[fields.length];

            final String id = ConvertToStringTransformer.transformValue(row.getValue(documentId));
            logger.info("Id is {}", id);
            if (StringUtils.isNullOrEmpty(id)) {
                return result;
            }

            final GetRequest request = new GetRequestBuilder(client).setId(id).setType(documentType).setFields(fields)
                    .setIndex(elasticsearchDatastore.getIndexName()).setOperationThreaded(false).request();
            final ActionFuture<GetResponse> getFuture = client.get(request);
            final GetResponse response = getFuture.actionGet();
            logger.info("Response is {}", response);
            if (!response.isExists()) {
                return result;
            }

            for (int i = 0; i < fields.length; i++) {
                final String field = fields[i];
                final GetField valueGetter = response.getField(field);
                if (valueGetter == null) {
                    logger.info("Document with id '{}' did not have the field '{}'", id, field);
                } else {
                    final Object value = valueGetter.getValue();
                    result[i] = ConvertToStringTransformer.transformValue(value);
                }
            }
            return result;
        } catch (Exception e) {
            logger.error("Exception while running the ElasticSearchDocumentIdLookupTransformer", e);
            throw e;
        }
    }
}
