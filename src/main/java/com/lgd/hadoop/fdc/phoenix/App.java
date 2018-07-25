package com.lgd.hadoop.fdc.phoenix;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    /*
    static final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    static final String IP = "40.78.62.72";
    static final String PORT = "2181";   
    static final String DB_URL = "jdbc:phoenix://" + IP + ":" + PORT + "/hbase-unsecure";
    */

    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final Properties properties = System.getProperties();
    
    public static void main( String[] args )
    {
        LOG.debug("LGD Hadoop2 DevCode");
                
     // Create variables
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try {
            
            properties.load(classLoader.getResourceAsStream("db.properties"));
            Class.forName(properties.getProperty("phoenix.driver"));

            LOG.debug("connecting to database..");
            
            // Connect to the database
            //connection = DriverManager.getConnection("jdbc:phoenix:localhost");
            //DriverManager.getConnection("jdbc:phoenix:zk1,zk2,zk3:2181:/hbase-unsecure");
            //connection = DriverManager.getConnection(DB_URL);
            //connection = DriverManager.getConnection("jdbc:phoenix:40.78.62.72:2181/hbase");
            connection = DriverManager.getConnection(properties.getProperty("phoenix.driver")); 
            connection = DriverManager.getConnection(properties.getProperty("phoenix.url"));
            LOG.debug("Creating statement...");
            
            // Create a JDBC statement
            statement = connection.createStatement();

            // Execute our statements
            statement.executeUpdate("create table javatest (mykey integer not null primary key, mycolumn varchar)");
            statement.executeUpdate("upsert into javatest values (1,'Hello')");
            statement.executeUpdate("upsert into javatest values (2,'Java Application')");
            connection.commit();

            // Query for table
            ps = connection.prepareStatement("select * from javatest");
            rs = ps.executeQuery();
            System.out.println("Table Values");
            while(rs.next()) {
                Integer myKey = rs.getInt(1);
                String myColumn = rs.getString(2);
                System.out.println("\tRow: " + myKey + " = " + myColumn);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();


        } catch(SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if(ps != null) {
                try {
                    ps.close();
                }
                catch(Exception e) {}
            }
            if(rs != null) {
                try {
                    rs.close();
                }
                catch(Exception e) {}
            }
            if(statement != null) {
                try {
                    statement.close();
                }
                catch(Exception e) {}
            }
            if(connection != null) {
                try {
                    connection.close();
                }
                catch(Exception e) {}
            }
        }


    }

}
