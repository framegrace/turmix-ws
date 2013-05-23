/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ideeli.turmix.resources;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ServletContext implements ServletContextListener {


    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "STARTING UP TURMIX");
    }//end contextInitialized method

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        Logger.getLogger(ServletContext.class.getName()).log(Level.SEVERE, "STOPPING TURMIX");
    }//end constextDestroyed method
}
