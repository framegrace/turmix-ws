package com.ideeli.turmix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrServerException;

/**
 *
 * @author marc
 */
public class IndexerAction {

    void refreshIndex() {
        Connection c = null;
        try {
            Logger.getLogger(IndexerAction.class.getName()).log(Level.INFO,"-- Refreshing --");
            c = CommonResources.connectionPool.getConnection();
            CommonResources.server.deleteByQuery("*:*");
            CommonResources.pdba.pushToSolr();
            CommonResources.dba.pushToSolr(c);
            CommonResources.server.commit();
            Logger.getLogger(IndexerAction.class.getName()).log(Level.INFO,"-- Refreshed OK --");
        } catch (SolrServerException | IOException | TurmixException | SQLException | RuntimeException ex) {
            Logger.getLogger(IndexerAction.class.getName()).log(Level.SEVERE, null, ex);
            try {
                Logger.getLogger(IndexerAction.class.getName()).log(Level.SEVERE, "Rolling back SolrUpdates");
                CommonResources.server.rollback();
            } catch (SolrServerException | IOException ex1) {
                Logger.getLogger(IndexerAction.class.getName()).log(Level.SEVERE, "Problem while rolling back", ex1);
            }
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ex) {
                    Logger.getLogger(IndexerAction.class.getName()).log(Level.SEVERE, "Connection failed on close", ex);
                }
            }
        }
    }
}
