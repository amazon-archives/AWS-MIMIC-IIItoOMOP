package com.amazon.sparkbatchprocessor;

import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.cloudformation.model.DeleteStackRequest;

/**
 *
 * @author hoodrya
 */
public class CloudFormationManager 
{
    private AmazonCloudFormationClient cf;
    
    public CloudFormationManager(Region region)
    {
        cf = new AmazonCloudFormationClient();
        cf.setRegion(region);    
    }
    
    public void terminateStack(String name) 
    {
        DeleteStackRequest deleteStackRequest = new DeleteStackRequest();
        
        deleteStackRequest.setStackName(name);
        
        cf.deleteStack(deleteStackRequest);
    }
}
