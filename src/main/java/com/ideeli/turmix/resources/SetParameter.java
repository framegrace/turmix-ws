package com.ideeli.turmix.resources;

import com.ideeli.turmix.CommonResources;
import com.ideeli.turmix.TurmixException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

/**
 *
 * @author marc
 */
@Path("/setParameter")
public class SetParameter {

    @GET
    @Produces("application/json")
    public String set(@QueryParam("host") String node,
            @QueryParam("name") String name,
            @QueryParam("value") String value) throws IOException {
        String err = "{\"retcode\": 0}";
        Connection c = null;
        try {
            c = CommonResources.getConnectionPool().getConnection();
            c.setAutoCommit(false);
            CommonResources.getDba().addVar(c, name, value, node);
            c.commit();
        } catch (SQLException ex) {
            Logger.getLogger(SetParameter.class.getName()).log(Level.SEVERE, null, ex);
            err = "{\"retcode\": 101,\"error\":\"" + ex.getMessage() + "\"}";
            if (c != null) {
                try {
                    c.rollback();
                } catch (SQLException ex1) {
                    Logger.getLogger(SetParameter.class.getName()).log(Level.SEVERE, null, ex1);
                    err = "{\"retcode\": 102,\"error\":\"" + ex1.getMessage() + "\"}";
                }
            }
        } catch (TurmixException ex) {
            Logger.getLogger(SetParameter.class.getName()).log(Level.SEVERE, null, ex);
            err = "{\"retcode\": 103,\"error\":\"" + ex.getMessage() + "\"}";
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                    Logger.getLogger(SetParameter.class.getName()).log(Level.SEVERE, null, e);
                    err = "{\"retcode\": 104,\"error\":\"" + e.getMessage() + "\"}";
                }
            }
        }
        return err;
    }
}
