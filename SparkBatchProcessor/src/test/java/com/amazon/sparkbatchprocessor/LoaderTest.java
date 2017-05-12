package com.amazon.sparkbatchprocessor;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.cloudformation.model.CreateStackRequest;
import com.amazonaws.services.cloudformation.model.StackSummary;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 *
 * @author hoodrya
 */
public class LoaderTest {
    
    static Loader loader;
    Configuration configuration;
    File target;
    
    @AfterClass
    public static void tearDownClass() 
    {
        new File("/tmp/spark-warehouse").delete();
    }
    
    @Before
    public void setUp() throws IOException, ClassNotFoundException, SQLException 
    {
        Parser parser = new Parser();
        target = new File("/tmp/flatfile.csv");
     
        loader = new Loader("JUnit test app", "local", "/tmp/spark-warehouse");
        configuration = parser.parse(getClass().getResourceAsStream("configloader.json"));
        
        if(target.exists()) target.delete();
        if (!Files.exists(target.toPath().getParent())) Files.createDirectories(target.toPath().getParent());
     
        Files.copy(getClass().getResourceAsStream("flatfile.csv"), target.toPath());
    }
    
    @After
    public void teardown()
    {
        target.delete();
    }
    
    /**
     * Test of teardown method, of class Loader.
     */
    @Test
    public void testTeardown() throws IOException, SQLException, ClassNotFoundException 
    {
        AmazonCloudFormationClient client = new AmazonCloudFormationClient();
        String name = "TestTeardownCluster" + UUID.randomUUID().toString();
        CreateStackRequest createStackRequest = new CreateStackRequest();
        
        client.setRegion(Region.getRegion(Regions.US_WEST_2)); 
        
        createStackRequest.setStackName(name);
        createStackRequest.setTemplateBody(IOUtils.toString(getClass().getResourceAsStream("cloudformation.template"), "UTF-8"));
        
        client.createStack(createStackRequest);
        
        loader.setConfiguration(configuration);
        loader.teardown(name, Region.getRegion(Regions.US_WEST_2));
        
        for(StackSummary stack : client.listStacks().getStackSummaries())
        {
            if(stack.getStackStatus().equalsIgnoreCase("DELETE_COMPLETE")) continue;
            if(stack.getStackStatus().equalsIgnoreCase("DELETE_FAILED")) continue;
            if(stack.getStackStatus().equalsIgnoreCase("DELETE_IN_PROGRESS")) continue;
            
            if(stack.getStackName().equals(name)) fail(name +  " should have been deleted but status is: " + stack.getStackStatus());
        }
    }

    @Test
    public void testLoadDf() throws IOException
    {
        Mapping mapping = configuration.getMappings().get(0);
        
        loader.setConfiguration(configuration);
        
        loader.loadSourceFiles(mapping);
        
        assertEquals(4l, loader.loadDf(getClass().getResourceAsStream("count.sql")).first().get(0));
        
        //order by second column descending and test top row of that resultset
        Dataset<Row> dataset = loader.loadDf(getClass().getResourceAsStream("sort.sql"));
        
        assertEquals("Doctor B", dataset.first().get(0));
        assertEquals("4", dataset.first().get(1));
        assertEquals("Clinic", dataset.first().get(2));
        assertEquals("Patient 3", dataset.first().get(3));
    }
    
    @Test
    public void testSoadSourceFiles() throws IOException
    {
        Mapping mapping = configuration.getMappings().get(0);
        
        loader.setConfiguration(configuration);
        
        loader.loadSourceFiles(mapping);
        
        Dataset<Row> dataset = loader.getSparkSession().sql("select * from flatfile");
        
        assertEquals(4, dataset.count());
        assertEquals("Doctor A", dataset.first().get(0));
        assertEquals("1", dataset.first().get(1));
        assertEquals("ED", dataset.first().get(2));
        assertEquals("Patient 2", dataset.first().get(3));
        
        //order by doctor descending and test top row of that resultset
        dataset = loader.getSparkSession().sql("select * from flatfile order by DoctorName desc");
        
        assertEquals("Doctor C", dataset.first().get(0));
        assertEquals("3", dataset.first().get(1));
        assertEquals("ED", dataset.first().get(2));
        assertEquals("Patient 1", dataset.first().get(3));
    }
    
    @Test
    public void testWrite() throws IOException
    {
        Mapping mapping = configuration.getMappings().get(1);
        Dataset<Row> data;
        
        loader.setConfiguration(configuration);
        loader.loadSourceFiles(mapping);
        loader.setDf(loader.loadDf(getClass().getResourceAsStream("write.sql")));
        
        loader.write("testwrite");
        
        data = loader.getSparkSession().read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("nullValue", "").option("treatEmptyValuesAsNulls","true").load(configuration.getFileSystem() + "://" + configuration.getSourceBucket() + "/stage/testwrite.csv" );
        
        assertEquals("Doctor A", data.first().get(0));
        assertEquals("1", data.first().get(1));
        assertEquals("ED", data.first().get(2));
        assertEquals("Patient 2", data.first().get(3));
        
        FileUtils.deleteDirectory(new File("/tmp/testwrite.csv"));
    }
    
    @Test
    public void testWriteWithOverflow() throws IOException
    {
     
        Mapping mapping = configuration.getMappings().get(1);
        Dataset<Row> overflow;
        Dataset<Row> data;
        
        loader.setConfiguration(configuration);
        loader.loadSourceFiles(mapping);
        loader.setDf(loader.loadDf(getClass().getResourceAsStream("write.sql")));
        
        //now test a file with overflow columns configured
        loader.write("testoverflow", mapping.getOverflowColumns());
        
        overflow = loader.getSparkSession().read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("nullValue", "").option("treatEmptyValuesAsNulls","true").load(configuration.getFileSystem() + "://" + configuration.getSourceBucket() + "/stage/column_overflow/testoverflow.csv" );
        overflow.createOrReplaceTempView("overflow");
        overflow = overflow.sqlContext().sql("select * from overflow where PatientId == 'Patient 3'");
        
        data = loader.getSparkSession().read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("nullValue", "").option("treatEmptyValuesAsNulls","true").load(configuration.getFileSystem() + "://" + configuration.getSourceBucket() + "/stage/testoverflow.csv" );
        data.createOrReplaceTempView("data");
        data = data.sqlContext().sql("select * from data where DoctorName == 'Doctor B'");
        
        assertEquals("file:///tmp/testoverflow.csv", overflow.first().get(1));
        assertEquals("Clinic", overflow.first().get(2));
        assertEquals("Patient 3", overflow.first().get(3));
        assertEquals("Doctor B", data.first().get(0));
        assertEquals("4", data.first().get(1));
        assertEquals("file:///tmp/column_overflow/testoverflow.csv", data.first().get(3));
        
        //make sure the UUID's match between the two datasets
        assertEquals(overflow.first().get(0), data.first().get(2));
        
        //changing the row on the dataset and check that the UUID is not the same as a different row in the overflow file
        data = data.sqlContext().sql("select * from data where DoctorName == 'Doctor C'");
        assertNotEquals(overflow.first().get(0), data.first().get(2));
        
        FileUtils.deleteDirectory(new File("/tmp/column_overflow"));
        FileUtils.deleteDirectory(new File("/tmp/testoverflow.csv"));
    }
}
