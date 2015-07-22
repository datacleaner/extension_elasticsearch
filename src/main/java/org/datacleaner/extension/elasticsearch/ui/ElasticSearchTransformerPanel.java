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
package org.datacleaner.extension.elasticsearch.ui;

import org.apache.metamodel.schema.Schema;
import org.datacleaner.bootstrap.WindowContext;
import org.datacleaner.configuration.DataCleanerConfiguration;
import org.datacleaner.connection.Datastore;
import org.datacleaner.connection.DatastoreConnection;
import org.datacleaner.descriptors.ConfiguredPropertyDescriptor;
import org.datacleaner.extension.elasticsearch.ElasticSearchTransformer;
import org.datacleaner.job.builder.ComponentBuilder;
import org.datacleaner.job.builder.TransformerComponentBuilder;
import org.datacleaner.panels.TransformerComponentBuilderPanel;
import org.datacleaner.widgets.DCComboBox.Listener;
import org.datacleaner.widgets.properties.PropertyWidget;
import org.datacleaner.widgets.properties.PropertyWidgetFactory;
import org.datacleaner.widgets.properties.SingleDatastorePropertyWidget;
import org.datacleaner.widgets.properties.SingleTableNamePropertyWidget;

public class ElasticSearchTransformerPanel extends TransformerComponentBuilderPanel {

    private static final long serialVersionUID = 1L;

    private final SingleTableNamePropertyWidget _documentTypeWidget;
    private final SingleDatastorePropertyWidget _datastoreWidget;

    private final ConfiguredPropertyDescriptor _datastoreProperty;
    private final ConfiguredPropertyDescriptor _documentTypeProperty;

    public ElasticSearchTransformerPanel(TransformerComponentBuilder<ElasticSearchTransformer> tcb,
            WindowContext windowContext, PropertyWidgetFactory propertyWidgetFactory,
            DataCleanerConfiguration configuration) {
        super(tcb, windowContext, propertyWidgetFactory, configuration);

        _datastoreProperty = tcb.getDescriptor().getConfiguredProperty(ElasticSearchTransformer.PROPERTY_ES_DATASTORE);
        _documentTypeProperty = tcb.getDescriptor().getConfiguredProperty(
                ElasticSearchTransformer.PROPERTY_DOCUMENT_TYPE);

        _datastoreWidget = new SingleDatastorePropertyWidget(tcb, _datastoreProperty,
                configuration.getDatastoreCatalog());
        _documentTypeWidget = new SingleTableNamePropertyWidget(tcb, _documentTypeProperty, windowContext);

        _datastoreWidget.addComboListener(new Listener<Datastore>() {

            @Override
            public void onItemSelected(Datastore datastore) {
                if (datastore != null) {
                    try (DatastoreConnection con = datastore.openConnection()) {
                        final Schema schema = con.getDataContext().getDefaultSchema();
                        _documentTypeWidget.setSchema(datastore, schema);
                    }
                }
            }
        });
    }

    @Override
    protected PropertyWidget<?> createPropertyWidget(ComponentBuilder componentBuilder,
            ConfiguredPropertyDescriptor propertyDescriptor) {
        if (propertyDescriptor == _datastoreProperty) {
            return _datastoreWidget;
        }
        if (propertyDescriptor == _documentTypeProperty) {
            return _documentTypeWidget;
        }
        return super.createPropertyWidget(componentBuilder, propertyDescriptor);
    }
}
