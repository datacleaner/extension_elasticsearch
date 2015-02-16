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
package org.eobjects.datacleaner.extension.elasticsearch.ui;

import javax.inject.Inject;

import org.datacleaner.api.Renderer;
import org.datacleaner.api.RendererBean;
import org.datacleaner.api.RendererPrecedence;
import org.datacleaner.guice.DCModule;
import org.datacleaner.job.builder.AnalyzerComponentBuilder;
import org.datacleaner.panels.AnalyzerComponentBuilderPresenter;
import org.datacleaner.panels.ComponentBuilderPresenterRenderingFormat;
import org.datacleaner.widgets.properties.PropertyWidgetFactory;
import org.eobjects.datacleaner.extension.elasticsearch.ElasticSearchIndexAnalyzer;

@RendererBean(ComponentBuilderPresenterRenderingFormat.class)
public class ElasticSearchIndexAnalyzerSwingRenderer implements
        Renderer<AnalyzerComponentBuilder<ElasticSearchIndexAnalyzer>, AnalyzerComponentBuilderPresenter> {

    @Inject
    DCModule dcModule;

    @Override
    public RendererPrecedence getPrecedence(AnalyzerComponentBuilder<ElasticSearchIndexAnalyzer> ajb) {
        Class<ElasticSearchIndexAnalyzer> componentClass = ajb.getDescriptor().getComponentClass();
        if (componentClass == ElasticSearchIndexAnalyzer.class) {
            return RendererPrecedence.HIGH;
        }
        return RendererPrecedence.NOT_CAPABLE;
    }

    @Override
    public AnalyzerComponentBuilderPresenter render(AnalyzerComponentBuilder<ElasticSearchIndexAnalyzer> ajb) {
        final PropertyWidgetFactory propertyWidgetFactory = dcModule.createChildInjectorForComponent(ajb).getInstance(
                PropertyWidgetFactory.class);
        return new ElasticSearchIndexAnalyzerJobPanel(ajb, propertyWidgetFactory);
    }

}
