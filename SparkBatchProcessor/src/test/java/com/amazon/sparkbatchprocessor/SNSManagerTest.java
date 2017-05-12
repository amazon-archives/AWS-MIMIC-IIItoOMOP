package com.amazon.sparkbatchprocessor;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author hoodrya
 */
public class SNSManagerTest {
    
    SNSManager instance = new SNSManager(Region.getRegion(Regions.US_WEST_2));
    AmazonSNSClient sns = new AmazonSNSClient();
    
    String success;  
    String failure;
    String successArn;
    String failureArn;
    String account;
    
    @Before
    public void setup()
    {
        sns.setRegion(Region.getRegion(Regions.US_WEST_2));

        success = "JunitSuccessTopic";
        failure =  "JunitFailureTopic"; 
        
        successArn = sns.createTopic(success).getTopicArn();
        failureArn = sns.createTopic(failure).getTopicArn();
        
        sns.setTopicAttributes(successArn, "DisplayName", success);
        sns.setTopicAttributes(failureArn, "DisplayName", failure);
    }
    
    @After
    public void teardown()
    {
        sns.deleteTopic(successArn);
        sns.deleteTopic(failureArn);
    }
    
    /**
     * Test of publish method, of class SNSManager.
     */
    @Test
    public void testPublish() 
    {
        String file = "JunitFile";
            
        assertNotNull(instance.publishSuccess(success, file));
        assertNotNull(instance.publishFailure(failure, file, "Fake stack trace"));
    }
}
