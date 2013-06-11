package com.ideeli.turmix;

import com.ideeli.turmix.indexer.IndexerTask;
import com.ideeli.turmix.indexer.IndexerPlugin;
import com.ideeli.turmix.indexer.plugins.NagiosIndexerPlugin;
import com.ideeli.turmix.indexer.plugins.PuppetDBIndexerPlugin;
import com.ideeli.turmix.indexer.plugins.PuppetDashboardIndexerPlugin;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    private static BoneCP connectionPool;
    private static DashboardActions dba = new DashboardActions();
    private static PuppetDBActions pdba = new PuppetDBActions();
    private static IndexerTask index = new IndexerTask();
    private static SolrServer server;
    private static HashMap<String, IndexerPlugin> IndexerPlugins = new HashMap<>();
    private static Properties configFile = new Properties();
    private static String puppetDBURL = "";
    private static boolean initialized = false;

    public static void initialize() {
        String configFilePath = "";
        try {
            configFilePath = System.getProperty("turmix.config.file");
            if (configFilePath == null) {
                configFilePath = "/etc/turmix/turmix.properties";
            }
            configFile.load(new FileInputStream(configFilePath));
            initialized = true;
            initializeIndexer();
            initializePool();
            initializeDashboard();
            initializeSolr();
        } catch (IOException | RuntimeException ex) {
            Logger.getLogger(CommonResources.class.getName()).log(Level.SEVERE, "Error reading config file " + configFilePath, ex);
        } catch (TurmixException te) {
            Logger.getLogger(CommonResources.class.getName()).log(Level.SEVERE, "Initialization error ", te);
        }
    }

    public static void addIndexerPlugin(IndexerPlugin ip) throws TurmixException {
        getIndexerPlugins().put(ip.getName(), ip);
    }

    private static void initializeDashboard() {
        puppetDBURL = configFile.getProperty("puppetdb.url");
    }

    private static void initializePool() {

        Connection connection = null;
        try {
            // load the database driver (make sure this is in your classpath!)
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            Logger.getLogger(CommonResources.class.getName()).log(Level.SEVERE, "DB connection error", e);
            return;
        }
        try {
            // setup the connection pool
            BoneCPConfig config = new BoneCPConfig();
            //"jdbc:mysql://localhost/dashboard"
            //"dashboard"
            //"98dashboard76"
            config.setJdbcUrl(configFile.getProperty("jdbc.url"));
            config.setUsername(configFile.getProperty("jdbc.user"));
            config.setPassword(configFile.getProperty("jdbc.pwd"));
            config.setCloseOpenStatements(true);
            config.setDetectUnclosedStatements(true);
            config.setMinConnectionsPerPartition(configFile.getProperty("jdbc.pool.min") == null ? 5 : new Integer(configFile.getProperty("jdbc.pool.min")));
            config.setMaxConnectionsPerPartition(configFile.getProperty("jdbc.pool.max") == null ? 10 : new Integer(configFile.getProperty("jdbc.pool.min")));
            config.setPartitionCount(1);
            connectionPool = new BoneCP(config);
        } catch (SQLException e) {
            Logger.getLogger(CommonResources.class.getName()).log(Level.SEVERE, "DB connection error", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    Logger.getLogger(CommonResources.class.getName()).log(Level.SEVERE, "Error closing connection", e);
                }
            }
        }
    }

    public static ArrayNode queryPuppetDB(String path, MultivaluedMap<String, String> params) throws IOException, TurmixException {
        WebResource resource = restClient.get().resource(puppetDBURL);
        String data_rw = resource.path(path).queryParams(params).accept("application/json").get(String.class);
        if (data_rw == null) {
            throw new TurmixException("Error on puppetDBClient connection. No data returned.");
        }
        ArrayNode an = (ArrayNode) mapper.readTree(data_rw);
        return an;
    }

    public static ArrayNode queryPuppetDB(String path, String ky, String vl) throws IOException, TurmixException {
        WebResource resource = restClient.get().resource(puppetDBURL);
        //params.add("format","json");
        String data_rw = resource.path(path).queryParam(ky, vl).accept("application/json").get(String.class);
        if (data_rw == null) {
            throw new TurmixException("Error on puppetDBClient connection. No data returned.");
        }
        ArrayNode an = (ArrayNode) mapper.readTree(data_rw);
        return an;
    }

    public static SolrDocument syncFields(String prefix, SolrDocument src, SolrDocument dest) {
        for (String dest_name : dest.getFieldNames()) {
            if (dest_name.startsWith(prefix)) {
                dest.setField(dest_name, null);
            }
        }
        for (String src_name : src.getFieldNames()) {
            dest.setField(prefix + "." + src_name, src.get(src_name));
        }
        return dest;
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

    public static void initializeSolr() {
        // "http://localhost:8080/solr/"
        server = new HttpSolrServer(configFile.getProperty("solr.url"));
    }

    /**
     * @return the connectionPool
     */
    public static BoneCP getConnectionPool() throws TurmixException {
        if (!initialized) {
            throw new TurmixException("Turmix unitialized");
        }
        return connectionPool;
    }

    /**
     * @return the dba
     */
    public static DashboardActions getDba() throws TurmixException {
        if (!initialized) {
            throw new TurmixException("Turmix unitialized");
        }
        return dba;
    }

    /**
     * @return the pdba
     */
    public static PuppetDBActions getPdba() throws TurmixException {
        if (!initialized) {
            throw new TurmixException("Turmix unitialized");
        }
        return pdba;
    }

    /**
     * @return the index
     */
    public static IndexerTask getIndex() throws TurmixException {
        if (!initialized) {
            throw new TurmixException("Turmix unitialized");
        }
        return index;
    }

    /**
     * @return the server
     */
    public static SolrServer getServer() throws TurmixException {
        if (!initialized) {
            throw new TurmixException("Turmix unitialized");
        }
        return server;
    }

    /**
     * @return the IndexerPlugins
     */
    public static HashMap<String, IndexerPlugin> getIndexerPlugins() throws TurmixException {
        if (!initialized) {
            throw new TurmixException("Turmix unitialized");
        }
        return IndexerPlugins;
    }
    static ScheduledExecutorService scheduleTaskExecutor;

    public static void startIndexer() {
        if (scheduleTaskExecutor == null) {
            int period=configFile.getProperty("indexer.period")==null?1:new Integer(configFile.getProperty("indexer.period"));
            int executerpool=configFile.getProperty("indexer.pool")==null?5:new Integer(configFile.getProperty("indexer.pool"));
            scheduleTaskExecutor = Executors.newScheduledThreadPool(executerpool);
            scheduleTaskExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                        try {
                            getIndex().refreshIndex();
                        } catch (TurmixException ex) {
                            Logger.getLogger(CommonResources.class.getName()).log(Level.SEVERE, "Cannot refresh Index", ex);
                            scheduleTaskExecutor.shutdown();
                        }
                }
            }, 1, period, TimeUnit.MINUTES);
        }
    }

    public static void stopIndexer() throws InterruptedException {
        if (scheduleTaskExecutor != null) {
            scheduleTaskExecutor.shutdown();
            scheduleTaskExecutor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    private static void initializeIndexer() throws TurmixException {
        CommonResources.addIndexerPlugin(new PuppetDBIndexerPlugin());
        CommonResources.addIndexerPlugin(new PuppetDashboardIndexerPlugin());
        CommonResources.addIndexerPlugin(new NagiosIndexerPlugin());
    }
}
