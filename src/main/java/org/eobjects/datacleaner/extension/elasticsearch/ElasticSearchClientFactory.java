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

import java.io.Closeable;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.eobjects.metamodel.util.LazyRef;

public class ElasticSearchClientFactory extends LazyRef<Client> implements Closeable {

    private TransportAddress[] _transportAddresses;
    private Settings _settings;
    private String _clusterName;
    private boolean isTransportClient = true;

    public ElasticSearchClientFactory(String[] hosts, String clusterName) {
        _clusterName = clusterName;
        if (hosts == null || hosts.length <= 0) {
            isTransportClient = false;
        } else {
            _transportAddresses = new TransportAddress[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                String hostname = hosts[i].trim();
                int port = 9300;

                int indexOfColon = hostname.indexOf(":");
                if (indexOfColon != -1) {
                    port = Integer.parseInt(hostname.substring(indexOfColon + 1));
                    hostname = hostname.substring(0, indexOfColon);
                }
                InetSocketTransportAddress transportAddress = new InetSocketTransportAddress(hostname, port);
                _transportAddresses[i] = transportAddress;
            }
            _settings = ImmutableSettings.builder().put("name", "DataCleaner").put("cluster.name", _clusterName).build();

        }

    }

    @Override
    protected Client fetch() throws Throwable {
        Client client;

        if (isTransportClient) {

            client = new TransportClient(_settings, false);

            for (TransportAddress transportAddress : _transportAddresses) {
                ((TransportClient) client).addTransportAddress(transportAddress);
            }
        } else {
            Node node = NodeBuilder.nodeBuilder().client(true).clusterName(_clusterName).node();
            client = node.client();

        }

        return client;
    }

    @Override
    public void close() {
        if (isFetched()) {
            get().close();
        }
    }
}
