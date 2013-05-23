/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.resources;

import com.ideeli.turmix.CommonResources;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;

/**
 * REST Web Service
 *
 * @author marc
 */
@Path("/reindex/")
public class reindex {

    @Context
    private UriInfo context;

    public reindex() {
    }

    @GET
    @Produces("text/plain")
    public String getText() {
        CommonResources.index.refreshIndex();
        return "OK";
    }

}
