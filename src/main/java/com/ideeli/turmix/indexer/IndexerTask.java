package com.ideeli.turmix.indexer;

import com.ideeli.turmix.CommonResources;
import com.ideeli.turmix.TurmixException;
import com.ideeli.turmix.indexer.plugins.NagiosIndexerPlugin;
import com.ideeli.turmix.indexer.plugins.PuppetDBIndexerPlugin;
import com.ideeli.turmix.indexer.plugins.PuppetDashboardIndexerPlugin;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author marc
 */
public class IndexerTask {

    ObjectMapper mapper = new ObjectMapper();

    public void initialize() throws TurmixException {
        // We could automate loading in the future
        CommonResources.addIndexerPlugin(new PuppetDBIndexerPlugin());
        CommonResources.addIndexerPlugin(new PuppetDashboardIndexerPlugin());
        CommonResources.addIndexerPlugin(new NagiosIndexerPlugin());
    }

    public void refreshIndex() {
        try {
            Collection<IndexerPlugin> plugins = CommonResources.getIndexerPlugins().values();
            Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "-- Refreshing --");
            Set<String> nodes = new HashSet();
            for (IndexerPlugin ip : plugins) {
                try {
                    Set mset = ip.listNodes();
                    nodes.addAll(mset);
                } catch (Exception e) {
                    Logger.getLogger(IndexerTask.class.getName()).log(Level.WARNING, "Error getting node list on " + ip.getName() + " IndexerPlugin",e);
                }
            }            
            int hosts = nodes.size();
            for (String host : nodes) {
                boolean done = false;
                // Optimistic locking 
                while (!done) {
                    SolrQuery query = new SolrQuery();
                    query.setQuery("host:" + host);
                    QueryResponse rsp = CommonResources.getServer().query(query);
                    SolrDocument dest;
                    if (rsp.getResults().size() > 0) {
                        dest = rsp.getResults().get(0);
                    } else {
                        dest = new SolrDocument();
                        dest.setField("host", host);
                    }
                    for (IndexerPlugin ip : plugins) {
                        try {
                            dest = ip.syncDocFields(host, dest);
                            Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "{0} Processed OK", ip.getName());
                        } catch (Exception e) {
                            Logger.getLogger(IndexerTask.class.getName()).log(Level.WARNING, "Error processing " + ip.getName() + " IndexerPlugin",e);
                        }
                    }
                    SolrInputDocument sid = ClientUtils.toSolrInputDocument(dest);
                    try {
                        CommonResources.getServer().add(sid);
                        done = true;
                    } catch (SolrException se) {
                        // Optimistic locking 
                        if (se.getMessage().contains("version conflict")) {
                            System.out.println("Node changed while updating. Repeating.");
                        } else {
                            Logger.getLogger(IndexerTask.class.getName()).log(Level.WARNING, "Error processing "+host,se);
                        }
                    }
                }
            }
            
            CommonResources.getServer().commit();
            Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "-- "+hosts+" hosts Refreshed OK --");
        } catch (SolrServerException | TurmixException | IOException | RuntimeException ex) {
            Logger.getLogger(IndexerTask.class.getName()).log(Level.SEVERE, "Error updating", ex);
        }
    }

    public static void main(String[] args) throws TurmixException {
        CommonResources.initialize();
        IndexerTask ia = new IndexerTask();
        ia.initialize();
        for (int i = 0; i < 200; i++) {
            ia.refreshIndex();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(IndexerTask.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
