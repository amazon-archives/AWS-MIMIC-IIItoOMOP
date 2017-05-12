package com.amazon.redshiftcopier;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author hoodrya
 */
public class FileProcessor {

    private final String bucket = System.getenv("bucket");
    private final AmazonS3 client;
    private final Connection connection;
    private Statement statement;
    private final String table;
    
    public FileProcessor(String table) throws ClassNotFoundException, SQLException 
    {
        client = new AmazonS3Client();
        client.setRegion(Region.getRegion(Regions.US_WEST_2));
        
        connection = new RedshiftConnection().getConnection();
        this.table = table;
    }

    public void readFiles() throws IOException, SQLException 
    {
        String query = "";
        S3Object s3object = client.getObject(new GetObjectRequest(bucket, System.getenv("prefix") +"/" + table + ".sql"));
        
        statement = connection.createStatement();
            
        query = IOUtils.toString(s3object.getObjectContent(), "UTF-8");
        query = query.replace("${redshift_arn}", System.getenv("redshift_arn"));
        query = query.replace("${bucket}", System.getenv("bucket"));
        
        statement.execute(query);
    }

    public void closeConnection() throws SQLException 
    {
        statement.close();
        connection.close();
    }
}
