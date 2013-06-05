package com.ideeli.turmix.indexer.plugins;

import com.ideeli.turmix.*;
import com.ideeli.turmix.indexer.IndexerPlugin;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.solr.common.SolrDocument;

public class NagiosIndexerPlugin implements IndexerPlugin {

    final HashMap<String,ArrayList<NagiosMetric>> metrics=new HashMap<>();
    
    final HashSet<String> hosts=new HashSet<>();
    
    @Override
    public HashSet listNodes() throws TurmixException {
        HashSet newHash;
        synchronized (hosts) {
            newHash = (HashSet) hosts.clone();
            hosts.clear();
        }
        return newHash;
    }

    @Override
    public SolrDocument syncDocFields(String host, SolrDocument dest) throws TurmixException {
        synchronized (metrics) {
            ArrayList<NagiosMetric> list = metrics.get(host);
            if (list != null) {
                SolrDocument docl = new SolrDocument();
                for (NagiosMetric nm : list) {
                    docl.addField(nm.name, nm.value);
                }
                dest = CommonResources.syncFields("nag", docl, dest);
            }
        }
        return dest;
    }
    
    public void addMetric(String host,String service,String Value) {
        synchronized(metrics) {
            ArrayList<NagiosMetric> list=metrics.get(host);
            if (list==null) {
                 list=new ArrayList<>();
                 metrics.put(host, list);
            } 
            list.add(new NagiosMetric(service, Value));
        }
        synchronized (hosts) {
            hosts.add(host);
        }
    }
    
    @Override
    public String getName() {
        return "nagios";
    }
    
    
}
class NagiosMetric {
    String name;
    String value;
    
    public NagiosMetric(String name,String value) {
        this.name=name;
        this.value=value;
    }
}