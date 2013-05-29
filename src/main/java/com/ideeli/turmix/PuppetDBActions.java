package com.ideeli.turmix;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.core.MultivaluedMap;
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

    
    public JsonNode readNodeData(String node) throws IOException, TurmixException {
        MultivaluedMap<String, String> data = new MultivaluedMapImpl();
        data.add("query", "[\"=\", \"certname\", \"" + node + "\"]");
        JsonNode result = CommonResources.queryPuppetDB("/v2/facts/", data);
        JsonNode rootNode = mapper.createObjectNode();
        for (JsonNode a : result) {
            ((ObjectNode) rootNode).put(a.get("name").getValueAsText(), a.get("value"));
        }
        return rootNode;
    }
    

     
    public Set<String> searchNodes(String params_rw) throws IOException, TurmixException {
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
}
