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

import org.datacleaner.api.InputColumn;
import org.datacleaner.descriptors.ConfiguredPropertyDescriptor;
import org.datacleaner.job.builder.AnalyzerComponentBuilder;
import org.datacleaner.job.builder.ComponentBuilder;
import org.datacleaner.panels.AnalyzerComponentBuilderPanel;
import org.datacleaner.widgets.properties.MultipleMappedStringsPropertyWidget;
import org.datacleaner.widgets.properties.PropertyWidget;
import org.datacleaner.widgets.properties.PropertyWidgetFactory;

public class ElasticSearchIndexAnalyzerJobPanel extends AnalyzerComponentBuilderPanel {

    private static final long serialVersionUID = 1L;

    private final MultipleMappedStringsPropertyWidget _mappedWidget;
    private final ConfiguredPropertyDescriptor _inputColumnsProperty;
    private final ConfiguredPropertyDescriptor _mappedStringsProperty;

    public ElasticSearchIndexAnalyzerJobPanel(AnalyzerComponentBuilder<?> analyzerJobBuilder,
            PropertyWidgetFactory propertyWidgetFactory) {
        super(analyzerJobBuilder, propertyWidgetFactory);

        _inputColumnsProperty = analyzerJobBuilder.getDescriptor().getConfiguredProperty("Values");
        _mappedStringsProperty = analyzerJobBuilder.getDescriptor().getConfiguredProperty("Fields");

        _mappedWidget = new MultipleMappedStringsPropertyWidget(analyzerJobBuilder, _inputColumnsProperty,
                _mappedStringsProperty) {
            @Override
            protected String getDefaultMappedString(InputColumn<?> inputColumn) {
                return inputColumn.getName();
            }
        };
    }

    @Override
    protected PropertyWidget<?> createPropertyWidget(ComponentBuilder componentBuilder,
            ConfiguredPropertyDescriptor propertyDescriptor) {
        if (propertyDescriptor == _inputColumnsProperty) {
            return _mappedWidget;
        } else if (propertyDescriptor == _mappedStringsProperty) {
            return _mappedWidget.getMappedStringsPropertyWidget();
        }
        return super.createPropertyWidget(componentBuilder, propertyDescriptor);
    }
}
