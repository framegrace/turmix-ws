/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.resources;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.PathParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;

/**
 * REST Web Service
 *
 * @author marc
 */
@Path("provision")
public class Provision {

    @Context
    private UriInfo context;

    /**
     * Creates a new instance of Provision
     */
    public Provision() {
    }

    /**
     * Retrieves representation of an instance of com.ideeli.turmix.resources.Provision
     * @return an instance of java.lang.String
     */
    @GET
    @Produces("text/plain")
    public String getText() {
        //TODO return proper representation object
        return "OKA!";
    }

    /**
     * PUT method for updating or creating an instance of Provision
     * @param content representation for the resource
     * @return an HTTP response with content of the updated or created resource.
     */
    @PUT
    @Consumes("text/plain")
    public void putText(String content) {
    }
}
