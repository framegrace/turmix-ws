package com.ideeli.turmix.indexer.plugins;

import com.ideeli.turmix.*;
import com.ideeli.turmix.indexer.IndexerPlugin;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

/**
 *
 * @author marc
 */
public class PuppetDashboardIndexerPlugin implements IndexerPlugin {

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public HashSet listNodes() throws TurmixException {
        Connection c = null;
        HashSet<String> result=new HashSet<>();
        try {
            c = CommonResources.getConnectionPool().getConnection();
            ArrayNode an=CommonResources.getDba().listNodes(c);
            for (JsonNode jn : an) {
                result.add(jn.get("name").getTextValue());
            }
            return result;
        } catch (SQLException ex) {
            throw new TurmixException("Error on PuppetDashboard on node list", ex);
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PuppetDashboardIndexerPlugin.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    @Override
    public SolrDocument syncDocFields(String host,SolrDocument dest) throws TurmixException {
        Connection c = null;
        try {
            c = CommonResources.getConnectionPool().getConnection();
            JsonNode s = CommonResources.getDba().getAllVars(c, host);
            SolrDocument docl = new SolrDocument();
            Iterator<String> ite = s.getFieldNames();
            while (ite.hasNext()) {
                String name = ite.next();
                String value = s.get(name).getTextValue();
                docl.setField(name, value);
            }
            // A very strange way to update multi-valued solr fields (arrays)
            HashSet classes = new HashSet();
            ArrayNode classes_j=CommonResources.getDba().getClasses(c, host);
            int i=0;
            for (JsonNode n : classes_j ) {
                classes.add(n.get("name").getTextValue());
            };
            docl.setField("tags", classes);
            dest=CommonResources.syncFields("dsb", docl, dest);
            return dest;
        } catch (SQLException ex) {
            throw new TurmixException("Error on PuppetDashboard doc sync",ex);
        } finally {
            if (c!=null) {
                try {
                    c.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PuppetDashboardIndexerPlugin.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    public String getName() {
        return "puppetDashboard";
    }
}
