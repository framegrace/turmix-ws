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
        //CommonResources.addIndexerPlugin(new PuppetDBIndexerPlugin());
        CommonResources.addIndexerPlugin(new PuppetDashboardIndexerPlugin());
        //CommonResources.addIndexerPlugin(new NagiosIndexerPlugin());
    }

    public void refreshIndex() {

        Collection<IndexerPlugin> plugins;
        try {
            plugins = CommonResources.getIndexerPlugins().values();
        } catch (TurmixException ex) {
            Logger.getLogger(IndexerTask.class.getName()).log(Level.SEVERE, "Error getting plugins", ex);
            return;
        }
        Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "-- Refreshing --");
        for (IndexerPlugin ip : plugins) {
            try {
                Set<String> nodes = ip.listNodes();
                if (nodes.size() > 0) {
                    for (String host : nodes) {
                        try {
                            Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "-- Processing HOST " + host + " on " + ip.getName());
                            indexHost(host, ip);
                        } catch (SolrServerException | TurmixException | IOException | RuntimeException ex) {
                            Logger.getLogger(IndexerTask.class.getName()).log(Level.SEVERE, "Error updating " + host + " on " + ip.getName() + " plugin. " + ex.getMessage());
                        }
                    }
                    Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "- " + nodes.size() + " hosts Refreshed for " + ip.getName() + " Indexer");
                }
            } catch (TurmixException | RuntimeException ex) {
                Logger.getLogger(IndexerTask.class.getName()).log(Level.SEVERE, "Error Indexing. " + ex.getMessage(), ex);
            }
        }
        Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "-- Refreshed --");
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

    public void indexHost(String host, String plugin_name) throws IOException, TurmixException, SolrServerException {
        indexHost(host, CommonResources.getIndexerPlugins().get(plugin_name));
    }
    
    public void indexHost(String host, IndexerPlugin ip) throws IOException, TurmixException, SolrServerException {
        boolean done = false;
        // Optimistic locking 
        
        Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "Indexing vars "+host);
        while (!done) {
            SolrQuery query = new SolrQuery();
            query.setQuery("host:" + host);
            QueryResponse rsp = CommonResources.getServer().query(query);
            SolrDocument dest;
            if (rsp.getResults().size() > 0) {
                dest = rsp.getResults().get(0);
                Logger.getLogger(IndexerTask.class.getName()).log(Level.FINE, "READED "+dest.toString());
            } else {
                dest = new SolrDocument();
                dest.setField("host", host);
                Logger.getLogger(IndexerTask.class.getName()).log(Level.FINE, "NEW "+dest.toString());
            }
            try {
                dest = ip.syncDocFields(host, dest);
                Logger.getLogger(IndexerTask.class.getName()).log(Level.FINE, "{0} Processed OK", ip.getName());
            } catch (Exception e) {
                Logger.getLogger(IndexerTask.class.getName()).log(Level.WARNING, "Error processing " + ip.getName() + " IndexerPlugin", e);
            }
            SolrInputDocument sid = ClientUtils.toSolrInputDocument(dest);
            try {
                CommonResources.getServer().add(sid);
                done = true;
            } catch (SolrException se) {
                // Optimistic locking 
                if (se.getMessage().contains("version conflict")) {
                     Logger.getLogger(IndexerTask.class.getName()).log(Level.FINE,"Node changed while updating. Repeating.");
                } else {
                    Logger.getLogger(IndexerTask.class.getName()).log(Level.WARNING, "Error processing "+host+": "+se.getMessage());
                    done=true;
                }
            }
        }
        CommonResources.getServer().commit();
    }
}
