package com.ideeli.turmix;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 *
 * @author marc
 */
public class PuppetDBActions {

    ObjectMapper mapper = new ObjectMapper();

    public void pushToSolr() throws SolrServerException, IOException, TurmixException {
        ArrayNode nodes=CommonResources.pdba.listNodes();
        int hosts=nodes.size();
        int updated=0;
        for (JsonNode p : nodes) {
            String host=p.get("name").getTextValue();
            JsonNode s=CommonResources.pdba.readNodeData(host);
            SolrDocument docl=new SolrDocument();
            docl.addField("host",host);
            Iterator<String> ite=s.getFieldNames();
            while(ite.hasNext()) {
                String name=ite.next();
                docl.addField(name,s.get(name).getTextValue());
            }
        Logger.getLogger(PuppetDBActions.class.getName()).log(Level.INFO,">>>>>>>>>>>>Updating:"+docl.toString());            
            SolrQuery query = new SolrQuery();
            query.setQuery("host:" + host);
            QueryResponse rsp = CommonResources.server.query(query);
            
            boolean changed = false;
            SolrDocument dest ;
            if (rsp.getResults().size() > 0) {
                dest = rsp.getResults().get(0);
                changed=CommonResources.mergeSolrDocuments(docl, dest);
            } else {
                docl.setField("host", host);
                dest = docl;
                changed=true;
            }
            if (changed) {
                updated++;
                CommonResources.server.add(ClientUtils.toSolrInputDocument(dest));
            }
        }
        Logger.getLogger(IndexerAction.class.getName()).log(Level.INFO,"Puppet DB: "+updated+" of "+hosts+" updated.");
    }
    
    public JsonNode readNodeData(String node) throws IOException {
        MultivaluedMap<String, String> data = new MultivaluedMapImpl();
        data.add("query", "[\"=\", \"certname\", \"" + node + "\"]");
        JsonNode result = CommonResources.queryPuppetDB("/v2/facts/", data);
        JsonNode rootNode = mapper.createObjectNode();
        for (JsonNode a : result) {
            ((ObjectNode) rootNode).put(a.get("name").getValueAsText(), a.get("value"));
        }
        return rootNode;
    }
    
     public ArrayNode listNodes() throws IOException, TurmixException {
         ArrayNode an = (ArrayNode) CommonResources.queryPuppetDB("v2/nodes","","");
         return an;
     }
     
    public Set<String> searchNodes(String params_rw) throws IOException {
        String[] params = params_rw.split(",");
        String filter = "";
        String sep = "[ \"and\",";
        int terms = params.length;
        for (String comp : params) {
            String[] kvs = comp.split("=");
            if (kvs.length > 1) {
                    filter += sep + "[\"=\",[\"fact\",\"" + kvs[0] + "\"],\"" + kvs[1] + "\"]";
            }
            sep = ",";
        }
        Set<String> ret = new HashSet<String>();
        if ("".equals(filter)) {
            ret.add("*");
            return ret;
        } else {
            filter += "]";
        }
        MultivaluedMap<String, String> data = new MultivaluedMapImpl();
        data.add("query", filter);
        ArrayNode an = (ArrayNode) CommonResources.queryPuppetDB("v2/nodes", data);
        for (JsonNode p : an) {
            ret.add(p.get("name").getTextValue());
        }
        return ret;
    }
    
//    public static void main(String[] args) throws SolrServerException, IOException, TurmixException {
//        CommonResources.initialize();
//        PuppetDBActions pda=new PuppetDBActions();
//        pda.pushToSolr();
//    }
}
