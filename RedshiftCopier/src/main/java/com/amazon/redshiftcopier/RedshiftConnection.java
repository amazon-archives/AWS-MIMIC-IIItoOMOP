package com.amazon.redshiftcopier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author hoodrya
 */
public class RedshiftConnection {
    
    private final String URL = System.getenv("endpoint"); 
    private final String PORT = System.getenv("port"); 
    private final String DB = System.getenv("dbname"); 
    private final String USERNAME = System.getenv("user");
    private final String PASSWORD = System.getenv("password");
    
    private Connection connection;
    
    public RedshiftConnection() throws ClassNotFoundException, SQLException
    {
        Class.forName("com.amazon.redshift.jdbc41.Driver");
        Properties props = new Properties();

        props.setProperty("user", USERNAME);
        props.setProperty("password", PASSWORD);
        
        connection = DriverManager.getConnection("jdbc:redshift://" + URL + ":" + PORT + "/" + DB, props);
    }
    
    public Connection getConnection() throws ClassNotFoundException, SQLException
    {
        return connection;
    }
}
