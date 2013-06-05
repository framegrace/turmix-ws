package com.ideeli.turmix.resources;

import com.ideeli.turmix.CommonResources;
import com.ideeli.turmix.TurmixException;
import com.ideeli.turmix.indexer.plugins.NagiosIndexerPlugin;
import java.io.IOException;
import java.sql.Connection;
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
@Path("/setNagiosMetric")
public class SetNagiosMetric {

    @GET
    @Produces("text/html")
    public String set(@QueryParam("host") String node,
            @QueryParam("name") String name,
            @QueryParam("value") String value) throws IOException {
        
        String err = "{\"retcode\": 0}";
        try {
            NagiosIndexerPlugin nip=(NagiosIndexerPlugin)CommonResources.getIndexerPlugins().get("nagios");
            nip.addMetric(node, name, value);
        } catch (TurmixException ex) {
            err = "{\"retcode\": 100,\"error\":\"" + ex.getMessage() + "\"}";
            Logger.getLogger(SetNagiosMetric.class.getName()).log(Level.SEVERE, null, ex);
        }
        return err;
    }
}
