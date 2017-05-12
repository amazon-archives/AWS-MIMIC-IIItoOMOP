package com.amazon.sparkbatchprocessor;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.cloudformation.model.CreateStackRequest;
import com.amazonaws.services.cloudformation.model.StackSummary;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author hoodrya
 */
public class CloudFormationManagerTest {

    /**
     * Test of terminateCluster method, of class EMRManager.
     */
    @Test
    public void testTerminateStack() throws IOException {
        AmazonCloudFormationClient client = new AmazonCloudFormationClient();
        String name = "JUnitStack" + UUID.randomUUID().toString();
        CreateStackRequest createStackRequest = new CreateStackRequest();
        CloudFormationManager manager = new CloudFormationManager(Region.getRegion(Regions.US_WEST_2));
        
        client.setRegion(Region.getRegion(Regions.US_WEST_2));
        
        createStackRequest.setStackName(name);
        createStackRequest.setTemplateBody(IOUtils.toString(getClass().getResourceAsStream("cloudformation.template"), "UTF-8"));
        
        client.createStack(createStackRequest);
        
        manager.terminateStack(name);
        
        for(StackSummary stack : client.listStacks().getStackSummaries())
        {
            if(stack.getStackStatus().equalsIgnoreCase("DELETE_COMPLETE")) continue;
            if(stack.getStackStatus().equalsIgnoreCase("DELETE_FAILED")) continue;
            if(stack.getStackStatus().equalsIgnoreCase("DELETE_IN_PROGRESS")) continue;
            
            if(stack.getStackName().equals(name)) fail(name +  " should have been deleted but status is: " + stack.getStackStatus());
        }
    }
}
