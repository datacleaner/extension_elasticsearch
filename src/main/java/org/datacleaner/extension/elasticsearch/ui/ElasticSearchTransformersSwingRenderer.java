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

import javax.inject.Inject;

import org.datacleaner.api.Renderer;
import org.datacleaner.api.RendererBean;
import org.datacleaner.api.RendererPrecedence;
import org.datacleaner.bootstrap.WindowContext;
import org.datacleaner.configuration.DataCleanerConfiguration;
import org.datacleaner.extension.elasticsearch.ElasticSearchTransformer;
import org.datacleaner.guice.DCModule;
import org.datacleaner.job.builder.TransformerComponentBuilder;
import org.datacleaner.panels.ComponentBuilderPresenterRenderingFormat;
import org.datacleaner.panels.TransformerComponentBuilderPresenter;
import org.datacleaner.util.ReflectionUtils;
import org.datacleaner.widgets.properties.PropertyWidgetFactory;

@RendererBean(ComponentBuilderPresenterRenderingFormat.class)
public class ElasticSearchTransformersSwingRenderer implements
        Renderer<TransformerComponentBuilder<ElasticSearchTransformer>, TransformerComponentBuilderPresenter> {

    @Inject
    DCModule dcModule;

    @Inject
    WindowContext windowContext;

    @Inject
    DataCleanerConfiguration configuration;
    
    @Override
    public RendererPrecedence getPrecedence(TransformerComponentBuilder<ElasticSearchTransformer> tcb) {
        Class<ElasticSearchTransformer> componentClass = tcb.getDescriptor().getComponentClass();
        if (ReflectionUtils.is(componentClass, ElasticSearchTransformer.class)) {
            return RendererPrecedence.HIGH;
        }
        return RendererPrecedence.NOT_CAPABLE;
    }

    @Override
    public TransformerComponentBuilderPresenter render(TransformerComponentBuilder<ElasticSearchTransformer> tcb) {
        final PropertyWidgetFactory propertyWidgetFactory = dcModule.createChildInjectorForComponent(tcb).getInstance(
                PropertyWidgetFactory.class);
        return new ElasticSearchTransformerPanel(tcb, windowContext, propertyWidgetFactory, configuration, dcModule);
    }

}
