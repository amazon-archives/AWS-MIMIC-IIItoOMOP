package com.amazon.sparkbatchprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author hoodrya
 */
public class ConfigurationTest {
    
    Configuration configuration;
    
    @Before
    public void setUp() throws IOException 
    {
        ObjectMapper mapper = new ObjectMapper();
        configuration = mapper.readValue(this.getClass().getResourceAsStream("config.json"), Configuration.class);
    }
    
    /**
     * Test of getSourceBucket method, of class Configuration.
     */
    @Test
    public void testGetSourceBucket()  
    {
        assertEquals("bucket1-raw", configuration.getSourceBucket());   
    }

    /**
     * Test of getSourceFiles method, of class Configuration.
     */
    @Test
    public void testGetSourceFiles() {
        List<String> files = new ArrayList<String>();
        
        files.add("file1.csv");
        assertEquals(files, configuration.getMappings().get(0).getSourceFiles());
        
        files.add("file2.csv");
        assertEquals(files, configuration.getMappings().get(1).getSourceFiles());
    }

    /**
     * Test of getDestinationBucket method, of class Configuration.
     */
    @Test
    public void testGetDestinationBucket() 
    {
        assertEquals("bucket1-stage", configuration.getDestinationBucket());
    }

    /**
     * Test of getSqlBucket method, of class Configuration.
     */
    @Test
    public void testGetSqlBucket() 
    {
        assertEquals("sql-bucket", configuration.getSqlBucket());
    }
 
    /**
     * Test of getDataSource method, of class Configuration.
     */
    @Test
    public void testGetDataSource() 
    {
        assertEquals("mimic", configuration.getDataSource());
    }

    /**
     * Test of getSqlFile method, of class Configuration.
     */
    @Test
    public void testGetSqlFile() 
    {
        assertEquals("sql/sql1.sql", configuration.getMappings().get(0).getSqlFile());
        assertEquals("sql2.sql", configuration.getMappings().get(1).getSqlFile());
    }
    
    /**
     * Test of getSqlFile method, of class Configuration.
     */
    @Test
    public void testOverflowColumns() 
    {
        List<String> list = new ArrayList<String>();
        
        assertEquals(list, configuration.getMappings().get(0).getOverflowColumns());
        assertEquals(list, configuration.getMappings().get(1).getOverflowColumns());
        assertEquals("[overflow1, overflow2]", configuration.getMappings().get(2).getOverflowColumns().toString());
    }

    /**
     * Test of toString method, of class Configuration.
     */
    @Test
    public void testToString() 
    {
        System.out.println(configuration.toString());
        
        String expected = "***** Configuration Details *****\n" +
                            "Data Source=mimic\n" +
                            "Destination Bucket=bucket1-stage\n" +
                            "Source Bucket=bucket1-raw\n" +
                            "SQL Bucket=sql-bucket\n" +
                            "File System=file\n" +
                            "Mappings=[\n" +
                            "***** Mapping Details *****\n" +
                            "Source Files=[file1.csv]\n" +
                            "SQL File=sql/sql1.sql\n" +
                            "Overflow Columns=[]\n" +
                            "*****************************, \n" +
                            "***** Mapping Details *****\n" +
                            "Source Files=[file1.csv, file2.csv]\n" +
                            "SQL File=sql2.sql\n" +
                            "Overflow Columns=[]\n" +
                            "*****************************, \n" +
                            "***** Mapping Details *****\n" +
                            "Source Files=[file1.csv]\n" +
                            "SQL File=sql2.sql\n" +
                            "Overflow Columns=[overflow1, overflow2]\n" +
                            "*****************************]\n" +
                            "*****************************";

        assertEquals(expected, configuration.toString());
    }
}
