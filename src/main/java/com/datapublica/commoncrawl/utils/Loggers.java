/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.utils;

import java.io.OutputStreamWriter;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * A custom static logger used to override HADOOP Logger without using a log4j configuration file (Quite complicated on
 * EMR) This logger uses a console appender instead of a file appender since EMR saves the console logs into a file and
 * puts it in the personal bucket. This class's Logger is static and it is statically configured
 * 
 */
public class Loggers {

 // Static configuration using code instead of an external configuration file
    public static void setup() {

        // Name the logger's category
        Logger LOG = Logger.getLogger("MyLogger");

        // Define a console appender
        ConsoleAppender cAppender = new ConsoleAppender();

        // Use the System.out as a writer to get the logs saved in stdout log file by EMR
        cAppender.setWriter(new OutputStreamWriter(System.out));

        // Set the layout
        cAppender.setLayout(new PatternLayout("%c{1} %-5p [%t]: %m%n"));

        // Set the level
        LOG.setLevel(Level.INFO);

        // Add the console appender
        LOG.addAppender(cAppender);
    }
}
