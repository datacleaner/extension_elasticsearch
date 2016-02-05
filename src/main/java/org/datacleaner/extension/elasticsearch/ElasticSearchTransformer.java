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

import org.datacleaner.api.Transformer;

public interface ElasticSearchTransformer extends Transformer {

    public static final String PROPERTY_ES_DATASTORE = "ElasticSearch index";
    public static final String PROPERTY_DOCUMENT_TYPE = "Document type";
    public static final String CONNECTION_TYPE_ERROR = "This component requires the connection to ElasticSearch to be done as a NODE or via TRANSPORT protocol";
}
