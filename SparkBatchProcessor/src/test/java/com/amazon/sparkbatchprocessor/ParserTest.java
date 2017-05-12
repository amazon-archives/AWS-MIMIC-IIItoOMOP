package com.amazon.sparkbatchprocessor;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author hoodrya
 */
public class ParserTest {

    /**
     * Test of parse method, of class Parser.
     */
    @Test
    public void testParse() throws Exception {
        Parser parser = new Parser();
        
        Configuration configuration = parser.parse(getClass().getResourceAsStream("configparser.json"));
        //Configuration config = list.get(0);
        
        assertEquals(2, configuration.getMappings().size());
        
        assertEquals("parser-stage", configuration.getDestinationBucket());
        assertEquals("parser-raw", configuration.getSourceBucket());
        assertEquals("parserfile1.csv", configuration.getMappings().get(0).getSourceFiles().get(0));
        assertEquals("sql-bucket", configuration.getSqlBucket());
        assertEquals("sql1.sql", configuration.getMappings().get(0).getSqlFile());
        assertEquals("file", configuration.getFileSystem());
        
        
        //Test 2nd JSON Object within Mappings JSON Array which contains multiple source files
        assertEquals("file1.csv", configuration.getMappings().get(1).getSourceFiles().get(0));
        assertEquals("file2.csv", configuration.getMappings().get(1).getSourceFiles().get(1));
        assertEquals("sql2.sql", configuration.getMappings().get(1).getSqlFile());
    } 
}
