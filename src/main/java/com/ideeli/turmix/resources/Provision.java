/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.resources;

import com.ideeli.turmix.CommonResources;
import com.ideeli.turmix.TurmixException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

/**
 *
 * @author marc
 */
@Path("/provision")
public class Provision {

    @GET
    @Produces("text/html")
    public String newNode(@QueryParam("host") String host,
            @DefaultValue("") @QueryParam("classes") String classes,
            @DefaultValue("") @QueryParam("groups") String groups,
            @DefaultValue("-") @QueryParam("desc") String desc) throws IOException {
        String err = "{\"retcode\": 0}";
        Connection c = null;
        try {
            
            c = CommonResources.getConnectionPool().getConnection();
            c.setAutoCommit(false);
            int retcode = CommonResources.getDba().provisionMode(c, host, desc, classes,groups);
            err = "{\"retcode\": 0,\"id\":"+retcode+"}";
            c.commit();
            
        } catch (SQLException ex) {
            Logger.getLogger(Provision.class.getName()).log(Level.SEVERE, null, ex);
            err = "{\"retcode\": 101,\"error\":\"" + ex.getMessage() + "\"}";
            if (c != null) {
                try {
                    c.rollback();
                } catch (SQLException ex1) {
                    Logger.getLogger(Provision.class.getName()).log(Level.SEVERE, null, ex1);
                    err = "{\"retcode\": 102,\"error\":\"" + ex1.getMessage() + "\"}";
                }
            }
        } catch (TurmixException ex) {
            Logger.getLogger(Provision.class.getName()).log(Level.INFO, ex.getMessage());
            err = "{\"retcode\": 103,\"error\":\"" + ex.getMessage() + "\"}";
            if (c != null) {
                try {
                    c.rollback();
                } catch (SQLException e) {
                    Logger.getLogger(Provision.class.getName()).log(Level.SEVERE, null, e);
                    err = "{\"retcode\": 104,\"error\":\"" + e.getMessage() + "\"}";
                }
            }
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                    Logger.getLogger(Provision.class.getName()).log(Level.SEVERE, null, e);
                    err = "{\"retcode\": 104,\"error\":\"" + e.getMessage() + "\"}";
                }
            }
        }
        return err;
    }
}
