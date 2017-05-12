package com.amazon.redshiftcopier;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author hoodrya
 */
public class Driver {
    
    private static FileProcessor processor;
    
    public String lambda(SNSEvent event, Context context)
    {
        try
        {   
            List<String> list = new ArrayList<String>();
            
            for(SNSEvent.SNSRecord record : event.getRecords())
            {
                String table = record.getSNS().getMessage().split(" ")[0];
                
                processor = new FileProcessor(table);
                processor.readFiles();
           
                processor.closeConnection();
                
                list.add(table);
            }
            
            return list.toString() + " successfully loaded";
        }
        catch(IOException | ClassNotFoundException | SQLException ex){ return ex.toString(); }
    }
}
