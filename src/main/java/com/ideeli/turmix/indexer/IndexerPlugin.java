/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.indexer;

import com.ideeli.turmix.TurmixException;
import java.util.HashSet;
import org.apache.solr.common.SolrDocument;

/**
 *
 * @author marc
 */
public interface IndexerPlugin {
    
    public String getName();
    public HashSet listNodes() throws TurmixException;
    public SolrDocument syncDocFields(String host,SolrDocument dest) throws TurmixException;
}
