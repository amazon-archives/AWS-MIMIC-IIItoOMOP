package com.amazon.sparkbatchprocessor;

import static com.amazonaws.regions.Regions.getCurrentRegion;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hoodrya
 */
public class Driver 
{

    public static void main(String args[]) throws IOException
    {
        if(args.length != 3) 
        {
            System.out.println("[1] Config File Bucket (eg - myBucket)");
            System.out.println("[2] Config File Key (eg - myKey/config.json)");
            System.out.println("[3] Stack Name (eg - SparkBatchProcessing)");
            System.exit(1);
        }
            
        try 
        {
            Loader loader = new Loader(getCurrentRegion());
            
            loader.load(new Parser().parse(args[0], args[1]));
            
            //sleep for 5 mins to ensure all Lambda functions have completed
            try { Thread.sleep(300000); } 
            catch (InterruptedException ex) { }
            
            loader.teardown(args[2], getCurrentRegion());            
        } 
        catch (SQLException | ClassNotFoundException ex) { Logger.getLogger(Driver.class.getName()).log(Level.SEVERE, null, ex); }
    }
}
