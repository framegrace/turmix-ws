package com.ideeli.turmix.indexer.plugins;

import com.ideeli.turmix.*;
import com.ideeli.turmix.indexer.IndexerPlugin;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

public class PuppetDBIndexerPlugin implements IndexerPlugin {

    @Override
    public HashSet listNodes() throws TurmixException {
        HashSet<String> result = new HashSet<>();
        try {
            ArrayNode an = (ArrayNode) CommonResources.queryPuppetDB("v2/nodes", "", "");
            for (JsonNode jn : an) {
                result.add(jn.get("name").getTextValue());
            }
            return result;
        } catch (IOException ex) {
            throw new TurmixException("Error on PuppetDB on node list", ex);
        }
    }

    @Override
    public SolrDocument syncDocFields(String host, SolrDocument dest) throws TurmixException {
        try {
            JsonNode s = CommonResources.getPdba().readNodeData(host);
            SolrDocument docl = new SolrDocument();
            Iterator<String> ite = s.getFieldNames();
            while (ite.hasNext()) {
                String name = ite.next();
                docl.addField(name, s.get(name).getTextValue());
            }
            dest = CommonResources.syncFields("pdb", docl, dest);
            return dest;
        } catch (IOException ex) {
            throw new TurmixException("Error on PuppetDB doc sync", ex);
        }
    }
    
    @Override
    public String getName() {
        return "puppetDB";
    }
}
