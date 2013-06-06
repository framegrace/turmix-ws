package com.ideeli.turmix.indexer.plugins;

import com.ideeli.turmix.*;
import com.ideeli.turmix.indexer.IndexerPlugin;
import com.ideeli.turmix.indexer.IndexerTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.common.SolrDocument;

public class NagiosIndexerPlugin implements IndexerPlugin {

    final HashMap<String,ArrayList<NagiosMetric>> metrics=new HashMap<>();
    
    @Override
    public Set listNodes() throws TurmixException {
        HashSet newHash=new HashSet();
        synchronized (metrics) {
            newHash.addAll(metrics.keySet());
        }
        return newHash;
    }

    @Override
    public SolrDocument syncDocFields(String host, SolrDocument dest)  {
        synchronized (metrics) {
            ArrayList<NagiosMetric> list = metrics.get(host);
            if (list != null) {
                SolrDocument docl = new SolrDocument();
                Logger.getLogger(IndexerTask.class.getName()).log(Level.INFO, "Host "+host+" has "+list.size()+" Metrics ");
                for (NagiosMetric nm : list) {
                    docl.addField(nm.name, nm.value);
                }
                dest = CommonResources.syncFields("nag", docl, dest);
                dest.setField("nag.last_update_dt", "NOW");
            }
            metrics.remove(host);
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