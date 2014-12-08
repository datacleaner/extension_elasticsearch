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

import org.apache.metamodel.util.LazyRef;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchClientFactory extends LazyRef<Client> implements Closeable {

    private final TransportAddress[] _transportAddresses;
    private final Settings _settings;
    private Node _node;

    public ElasticSearchClientFactory(String[] hosts, String clusterName) {
        if (hosts == null || hosts.length == 0) {
            _settings = ImmutableSettings.settingsBuilder().put("http.enabled", true)
                    .put("node.name", "DataCleanerNode").put("cluster.name", clusterName).build();
            _transportAddresses = null;
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
            _settings = ImmutableSettings.builder().put("name", "DataCleaner").put("cluster.name", clusterName).build();

        }

    }

    /**
     * Parameterised constructor which just take clustername and provide client
     * based on cluster name itself. Use this only when you have elastic search
     * server running on same network as client.
     * 
     * @param clusterName
     */
    public ElasticSearchClientFactory(String clusterName) {
        this(null, clusterName);
    }

    @Override
    protected Client fetch() throws Throwable {

        if (_transportAddresses != null) {
            final TransportClient client = new TransportClient(_settings, false);

            for (TransportAddress transportAddress : _transportAddresses) {
                client.addTransportAddress(transportAddress);
            }

            return client;
        } else {
            
            _node = NodeBuilder.nodeBuilder().client(true).settings(_settings).node();
            return _node.client();
        }
    }

    @Override
    public void close() {
        if (isFetched()) {
            final Client client = get();
            client.close();
            if (_node != null) {
                _node.close();
            }

        }
    }
}
