/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.resources;

import com.ideeli.turmix.CommonResources;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ServletContext implements ServletContextListener {


    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "STARTING UP TURMIX");
        CommonResources.initialize();
        CommonResources.startIndexer();
    }//end contextInitialized method

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        try {
            Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "Stopping scheduler...");
            CommonResources.stopIndexer();
            Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "Scheduler stopped");
        } //end constextDestroyed method
        catch (InterruptedException ex) {
            Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//end constextDestroyed method
}
