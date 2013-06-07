package com.ideeli.turmix;

import com.ideeli.turmix.resources.Provision;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    static String GETGROUP = "select * from node_groups where name=?";
    static String ASSIGNGROUP = "insert into node_group_memberships values (null,?,?,now(),now())";
    static String GETGROUPS   = "select m.id,g.name from node_group_memberships m inner join node_groups g on (g.id=m.node_group_id) where m.node_id=? and m.node_group_id=?";
    //Connection c = CommonResources.connectionPool.getConnection();
    //        c.setAutoCommit(false);
    ObjectMapper mapper = new ObjectMapper();

    public int provisionMode(Connection c, String name, String desc, String classes_rw, String groups_rw) throws SQLException, TurmixException {
        if ("".equals(name)) throw new TurmixException("No name specified");
        int node_id = getNodeId(c, name);
        String[] classes = classes_rw.split(",");
        String[] groups  = groups_rw.split(",");
        Logger.getLogger(Provision.class.getName()).log(Level.INFO,"Provision : name="+name+" classes_rw="+classes_rw+" groups_rw="+groups_rw);
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
                    Logger.getLogger(Provision.class.getName()).log(Level.INFO,"Trying to assignClass : "+cls);
                    if (!"".equals(cls)) assignClass(c, cls, node_id);
                }
                for (String grp : groups) {
                    if (!"".equals(grp)) assignGroup(c,grp,node_id);
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

    public void assignClass(Connection c, String cls, String node) throws SQLException, TurmixException {
        int node_id = getNodeId(c, node);
        assignClass(c, cls, node_id);
    }
    
    public void assignGroup(Connection c, String cls, String node) throws SQLException, TurmixException {
        int node_id = getNodeId(c, node);
        assignGroup(c, cls, node_id);
    }
    
    public void assignGroup(Connection c, String grp, int node_id) throws SQLException, TurmixException {
        int group_id = getGroup(c, grp);
        PreparedStatement get_stmt = c.prepareStatement(GETGROUPS);
        get_stmt.setInt(1, node_id);
        get_stmt.setInt(2, group_id);
        ResultSet rs = get_stmt.executeQuery();
        if (!rs.next()) {
            PreparedStatement cnr_stmt = c.prepareStatement(ASSIGNGROUP);
            cnr_stmt.setInt(1, node_id);
            cnr_stmt.setInt(2, group_id);
            cnr_stmt.executeUpdate();
        }
    }
    
    public int getGroup(Connection c,String grp) throws SQLException, TurmixException {
        PreparedStatement get_stmt = c.prepareStatement(GETGROUP);
        get_stmt.setString(1, grp);
        ResultSet rs=get_stmt.executeQuery();
        int id=-1;
        if (rs.next()) {
            id=rs.getInt("id");
        } else throw new TurmixException("Group do not exists");
        return id;
    }
    
    public void assignClass(Connection c, String cls, int node_id) throws SQLException, TurmixException {
        int class_id = getDBClass(c, cls);
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

    public int getDBClass(Connection c,String classname) throws SQLException, TurmixException {
        PreparedStatement rc = c.prepareStatement(GETCLASS);
        rc.setString(1, classname);
        ResultSet class_rs = rc.executeQuery();
        int class_id = -1;
        if (class_rs.next()) {
            class_id = class_rs.getInt("id");
        } else throw new TurmixException("Class do not exists");
        return class_id;
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
            cnr_stmt.executeUpdate();
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

    public int addClass(Connection c, String classname) throws SQLException, TurmixException {
        int class_id = getDBClass(c, classname);
        PreparedStatement class_stmt = c.prepareStatement(ADDCLASS, Statement.RETURN_GENERATED_KEYS);
        class_stmt.setString(1, classname);
        class_stmt.executeUpdate();
        ResultSet rsc = class_stmt.getGeneratedKeys();
        if (rsc.next()) {
            class_id = rsc.getInt(1);
        }
        return class_id;
    }

    public ArrayNode getClasses(Connection c, String node) throws SQLException {
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
}
