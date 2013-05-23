package com.ideeli.turmix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class DashboardActions {

    static String ADDNODE = "insert into nodes values(null,?,?,NOW(),now(),null,null,null,0,null)";
    static String ADDCLASS = "insert into node_classes values(null,?,null,null)";
    static String GETCLASS = "select * from node_classes where name=?";
    static String GETNODES = "select * from nodes";
    static String GETNODE = "select * from nodes where name=?";
    static String CLASSONNODE = "insert into node_class_memberships values (null,?,?,now(),now())";
    static String GETCLASSONNODE = "select * from node_class_memberships where node_id=? and node_class_id=?";
    static String GETCLASSES = "select c.id,c.name from nodes n,node_classes c,node_class_memberships r where r.node_id=n.id and r.node_class_id=c.id and n.name=?";
    static String GETVAR = "select p.key,p.value from parameters p,nodes n where p.parameterable_id=n.id and p.parameterable_type='Node' and p.key=? and n.name=?";
    static String ADDVAR = "insert into parameters values(null,?,?,?,'Node',now(),now())";
    static String UPDATEVAR = "update parameters set value=? where parameterable_id=? and key=?";
    static String GET_ALL_VARS = "select p.key,p.value from parameters p,nodes n where p.parameterable_id=n.id and p.parameterable_type='Node' and n.name=?";
    static String SEARCH_NODES = "select parameterable_id,n.name from parameters p left join nodes n on (p.parameterable_id=n.id)";
    static String SEARCH_NODES_POST = "group by p.parameterable_id having count(parameterable_id)=? order by parameterable_id";
    //Connection c = CommonResources.connectionPool.getConnection();
    //        c.setAutoCommit(false);
    ObjectMapper mapper = new ObjectMapper();

    public int provisionMode(Connection c, String name, String desc, String classes_rw) throws SQLException, TurmixException {
        int node_id = getNodeId(c, name);
        String[] classes = classes_rw.split(",");
        if (node_id == -1) {
            PreparedStatement node_stmt = c.prepareStatement(ADDNODE, Statement.RETURN_GENERATED_KEYS);
            node_stmt.setString(1, name);
            node_stmt.setString(2, desc);
            node_stmt.executeUpdate();
            ResultSet rs = node_stmt.getGeneratedKeys();
            if (rs.next()) {
                node_id = rs.getInt(1);
            }
            if (node_id != -1) {
                for (String cls : classes) {
                    assignClass(c, cls, node_id);
                }
            }
        } else {
            throw new TurmixException("Node already exists (id:" + node_id + ")");
        }
        return node_id;
    }

    public int getNodeId(Connection c, String node) throws SQLException {
        PreparedStatement vars = c.prepareStatement(GETNODE);
        vars.setString(1, node);
        vars.executeQuery();
        ResultSet rsc = vars.getResultSet();
        if (rsc.next()) {
            return rsc.getInt("id");
        }
        return -1;
    }

    public void assignClass(Connection c, String cls, String node) throws SQLException {
        int node_id = getNodeId(c, node);
        assignClass(c, cls, node_id);

    }

    public void assignClass(Connection c, String cls, int node_id) throws SQLException {
        int class_id = addClass(c, cls);
        PreparedStatement get_stmt = c.prepareStatement(GETCLASSONNODE);
        get_stmt.setInt(1, node_id);
        get_stmt.setInt(2, class_id);
        ResultSet rs = get_stmt.executeQuery();
        if (!rs.next()) {
            PreparedStatement cnr_stmt = c.prepareStatement(CLASSONNODE);
            cnr_stmt.setInt(1, node_id);
            cnr_stmt.setInt(2, class_id);
            cnr_stmt.executeUpdate();
        }
    }

    public void addVar(Connection c, String name, String value, String node) throws SQLException, TurmixException {
        int node_id = getNodeId(c, node);
        if (node_id != -1) {
            addVar(c, name, value, node, node_id);
        } else {
            throw new TurmixException("Node do not exists");
        }
    }

    public void addVar(Connection c, String name, String value, String node, int node_id) throws SQLException, TurmixException {
        JsonNode oldvalue = getVar(c, node, name);
        if (!oldvalue.has(name)) {
            PreparedStatement cnr_stmt = c.prepareStatement(ADDVAR);
            cnr_stmt.setString(1, name);
            cnr_stmt.setString(2, value);
            cnr_stmt.setInt(3, node_id);
            cnr_stmt.executeUpdate();
        } else {
            PreparedStatement cnr_stmt = c.prepareStatement(UPDATEVAR);
            cnr_stmt.setString(1, value);
            cnr_stmt.setInt(2, node_id);
            cnr_stmt.setString(3, name);
        }
    }

    public Set<String> searchNodes(Connection c, String params_rw) throws SQLException {
        Set<String> ret = new HashSet<>();
        String[] params = params_rw.split(",");
        String filter = "";
        String sep = " WHERE (";
        int terms = params.length;
        for (String comp : params) {
            String[] kvs = comp.split("=");
            if (kvs.length > 1) {
                filter += sep + "( p.key='" + kvs[0] + "' and p.value='" + kvs[1] + "') ";
                sep = "OR";
            }
        }
        if ("".equals(filter)) {
            ret.add("*");
            return ret;
        } else {
            filter += ")";
        }
        PreparedStatement vars = c.prepareStatement(SEARCH_NODES + filter + SEARCH_NODES_POST);
        vars.setInt(1, terms);
        vars.executeQuery();
        ResultSet rsc = vars.getResultSet();
        while (rsc.next()) {
            ret.add(rsc.getString(2));
        }
        return ret;
    }

    public int addClass(Connection c, String classname) throws SQLException {
        PreparedStatement rc = c.prepareStatement(GETCLASS);
        rc.setString(1, classname);
        ResultSet class_rs = rc.executeQuery();
        int class_id = -1;
        if (class_rs.next()) {
            class_id = class_rs.getInt("id");
        }
        if (class_id == -1) {
            PreparedStatement class_stmt = c.prepareStatement(ADDCLASS, Statement.RETURN_GENERATED_KEYS);
            class_stmt.setString(1, classname);
            class_stmt.executeUpdate();
            ResultSet rsc = class_stmt.getGeneratedKeys();
            if (rsc.next()) {
                class_id = rsc.getInt(1);
            }
        }
        return class_id;
    }

    public JsonNode getClasses(Connection c, String node) throws SQLException {
        PreparedStatement classes = c.prepareStatement(GETCLASSES);
        classes.setString(1, node);
        classes.executeQuery();
        ResultSet rsc = classes.getResultSet();
        ArrayNode ret = mapper.createArrayNode();
        while (rsc.next()) {
            JsonNode rowNode = mapper.createObjectNode();
            ((ObjectNode) rowNode).put("name", rsc.getString(2));
            ret.add(rowNode);
        }
        return ret;
    }

    public ArrayNode listNodes(Connection c) throws SQLException {
        PreparedStatement classes = c.prepareStatement(GETNODES);
        classes.executeQuery();
        ResultSet rsc = classes.getResultSet();
        ArrayNode ret = mapper.createArrayNode();
        while (rsc.next()) {
            JsonNode rowNode = mapper.createObjectNode();
            ((ObjectNode) rowNode).put("name", rsc.getString("name"));
            ret.add(rowNode);
        }
        return ret;
    }

    public JsonNode getVar(Connection c, String node, String param) throws SQLException {
        PreparedStatement vars = c.prepareStatement(GETVAR);
        vars.setString(1, node);
        vars.setString(2, param);
        vars.executeQuery();
        ResultSet rsc = vars.getResultSet();
        JsonNode rootNode = mapper.createObjectNode();
        if (rsc.next()) {
            ((ObjectNode) rootNode).put(param, rsc.getString(2));
        }
        return rootNode;
    }

    public JsonNode getAllVars(Connection c, String node) throws SQLException {
        HashMap<String, String> ret = new HashMap<>();
        PreparedStatement vars = c.prepareStatement(GET_ALL_VARS);
        vars.setString(1, node);
        vars.executeQuery();
        ResultSet rsc = vars.getResultSet();
        JsonNode rootNode = mapper.createObjectNode();
        while (rsc.next()) {
            ((ObjectNode) rootNode).put(rsc.getString(1), rsc.getString(2));
        }
        return rootNode;
    }

    public void pushToSolr(Connection c) throws SolrServerException, IOException, SQLException {
        ArrayNode nodes = listNodes(c);
        int hosts = nodes.size();
        int updated = 0;
        for (JsonNode Jhost : nodes) {
            String host = Jhost.get("name").getTextValue();
            JsonNode s = getAllVars(c, host);
            SolrDocument docl = new SolrDocument();
            Iterator<String> ite = s.getFieldNames();
            while (ite.hasNext()) {
                String name = ite.next();
                String value = s.get(name).getTextValue();
                docl.setField(name, value);
            }
            SolrQuery query = new SolrQuery();
            query.setQuery("host:" + host);
            QueryResponse rsp = CommonResources.server.query(query);
            boolean changed = false;
            SolrDocument dest = null;
            if (rsp.getResults().size() > 0) {
                dest = rsp.getResults().get(0);
                changed = CommonResources.mergeSolrDocuments(docl, dest);
            } else {
                docl.setField("host", host);
                dest = docl;
                changed = true;
            }
            if (changed) {
                CommonResources.server.add(ClientUtils.toSolrInputDocument(dest));
                updated++;
            }
        }
        Logger.getLogger(IndexerAction.class.getName()).log(Level.INFO,"Puppet Dashboard: " + updated + " of " + hosts + " updated.");
    }
}
