package com.amazon.sparkbatchprocessor;

import com.amazonaws.regions.Region;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.Topic;

/**
 *
 * @author hoodrya
 */
public class SNSManager {
    
    private final AmazonSNSClient sns;
    
    public SNSManager(Region region)
    {
        sns = new AmazonSNSClient();
        sns.setRegion(region);
    }
    
    public PublishResult publishSuccess(String topicName, String table) 
    {
        PublishResult result = null;
        
        for(Topic topic : sns.listTopics().getTopics())
        {
            if(sns.getTopicAttributes(topic.getTopicArn()).getAttributes().get("DisplayName").equals(topicName))
            {
                PublishRequest publishRequest = new PublishRequest(topic.getTopicArn(), table + " table has been staged in S3");
        
                result = sns.publish(publishRequest);
            }
            
        }
        return result;
    }
    
    public PublishResult publishFailure(String topicName, String table, String stack) 
    {
        PublishResult result = null;
        
        for(Topic topic : sns.listTopics().getTopics())
        {
            if(sns.getTopicAttributes(topic.getTopicArn()).getAttributes().get("DisplayName").equals(topicName))
            {
                PublishRequest publishRequest = new PublishRequest(topic.getTopicArn(), "[ERROR]: " + table + " table was not able to be staged in S3. \n\n" + stack);
        
                result = sns.publish(publishRequest);
            }
            
        }
        return result;
    }
}
