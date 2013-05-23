/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

/**
 *
 * @author marc
 */
public class SolrBackend {

    /**
     *    <field name="uri" type="string" indexed="true" stored="true" required="true" multiValued="false" />
   <field name="id" type="int" indexed="true" stored="true" required="true" multiValued="false" />
   <field name="host" type="string" indexed="true" stored="true"/>
   <field name="type" type="string" indexed="true" stored="true" omitNorms="true"/>
   <field name="name" type="string" indexed="true" stored="true" omitNorms="true"/>
   <field name="value" type="text_general" indexed="true" stored="true" omitNorms="true"/>

     */
 
    public static void main(String[] args) throws FileNotFoundException, SolrServerException, IOException, TurmixException {
        SolrServer server = new HttpSolrServer("http://localhost:8080/solr/");
        ArrayNode nodes=CommonResources.pdba.listNodes();
        for (JsonNode p : nodes) {
            String host=p.get("name").getTextValue();
            System.out.println("--->"+host);
            JsonNode s=CommonResources.pdba.readNodeData(host);
            SolrInputDocument docl=new SolrInputDocument();
            docl.setField("uri", host+".facts");
            docl.setField("type","facts");
            docl.setField("host",host);
            Iterator<String> ite=s.getFieldNames();
            while(ite.hasNext()) {
                String name=ite.next();
                docl.addField(name,s.get(name));
            }
            server.add(docl);
            System.out.println(s);
        }
        server.commit();
    }
}
