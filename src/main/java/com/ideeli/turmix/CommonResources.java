package com.ideeli.turmix;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 *
 * @author marc
 */
public class CommonResources {


    public static final ThreadLocal<Client> restClient = new ThreadLocal<Client>() {
        @Override
        protected Client initialValue() {
            ClientConfig clientConfig = new DefaultClientConfig();
            return Client.create(clientConfig);
        }
    };
    public static ObjectMapper mapper = new ObjectMapper();
    public static BoneCP connectionPool;
    public static DashboardActions dba = new DashboardActions();
    public static PuppetDBActions pdba = new PuppetDBActions();
    public static IndexerAction index = new IndexerAction();
    public static SolrServer server;

    public static void initialize() {
        initializePool();
        //initializeFacts();
        server = new HttpSolrServer("http://localhost:8080/solr/");
    }

    private static void initializePool() {

        Connection connection = null;
        try {
            // load the database driver (make sure this is in your classpath!)
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            // setup the connection pool
            BoneCPConfig config = new BoneCPConfig();
            config.setJdbcUrl("jdbc:mysql://localhost/dashboard");
            config.setUsername("dashboard");
            config.setPassword("98dashboard76");
            config.setMinConnectionsPerPartition(5);
            config.setMaxConnectionsPerPartition(10);
            config.setPartitionCount(1);
            connectionPool = new BoneCP(config);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static ArrayNode queryPuppetDB(String path, MultivaluedMap<String, String> params) throws IOException {
        WebResource resource = restClient.get().resource("http://localhost:8888/");
        //params.add("format","json");
        String data_rw = resource.path(path).queryParams(params).accept("application/json").get(String.class);
        ArrayNode an = (ArrayNode) mapper.readTree(data_rw);
        return an;
    }

    public static ArrayNode queryPuppetDB(String path, String ky, String vl) throws IOException, TurmixException {
        WebResource resource = restClient.get().resource("http://localhost:8888/");
        //params.add("format","json");
        String data_rw = resource.path(path).queryParam(ky, vl).accept("application/json").get(String.class);
        if (data_rw == null) {
            throw new TurmixException("Error on puppetDBClient connection. No data returned.");
        }
        ArrayNode an = (ArrayNode) mapper.readTree(data_rw);
        return an;
    }

//    public static String convert(String format, Object data) throws IOException {
//        String out = "";
//        switch (format) {
//            case "yaml":
//                Yaml yaml = new Yaml();
//                out = yaml.dump(data);
//                break;
//            case "json":
//                out = CommonResources.mapper.writeValueAsString(data);
//        }
//        return out;
//    }

    public static boolean mergeSolrDocuments(SolrDocument src, SolrDocument dest) {
        boolean changed = false;
        for (String src_name : src.getFieldNames()) {
            Object dst_value = dest.get(src_name);
            Object src_value = src.get(src_name);
            // Unnecesary null test, but left it there for clarity
            if ((dst_value == null) || (dst_value != null && !dst_value.equals(src_value))) {
                dest.setField(src_name, src_value);
                changed = true;
            }
        }
        return changed;
    }

    public static JsonNode mergeJsonDocuments(JsonNode destination, JsonNode source) {

        Iterator<String> fieldNames;
        fieldNames = source.getFieldNames();
        while (fieldNames.hasNext()) {

            String fieldName = fieldNames.next();
            JsonNode jsonNode = destination.get(fieldName);
            // if field doesn't exist or is an embedded object
            if (jsonNode != null && jsonNode.isObject()) {
                mergeJsonDocuments(jsonNode, source.get(fieldName));
            } else {
                if (destination instanceof ObjectNode) {
                    // Overwrite field
                    JsonNode value = source.get(fieldName);
                    ((ObjectNode) destination).put(fieldName, value);
                }
            }

        }
        return destination;
    }
}
