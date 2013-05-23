/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.resources;

import com.ideeli.turmix.CommonResources;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ServletContext implements ServletContextListener {


    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "STARTING UP TURMIX");
        CommonResources.initialize();
                ScheduledExecutorService scheduleTaskExecutor = Executors.newScheduledThreadPool(5);
        scheduleTaskExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                CommonResources.index.refreshIndex();
            }
        }, 0, 1, TimeUnit.MINUTES);
    }//end contextInitialized method

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "STOPPING TURMIX");
    }//end constextDestroyed method
}
