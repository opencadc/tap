package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.tap.db.TableCreatorTest.SimpleRowMapper;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.io.AsciiTableData;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author pdowler
 */
public class TableLoaderTest extends TestUtil {

    private static final Logger log = Logger.getLogger(TableLoaderTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
    }
    
    public TableLoaderTest() { 
        super();
    }
    
    //@Test
    public void testPostNoTableName() {
        Assert.fail("not yet written");
    }
    
    //@Test
    public void testPostNoColumnRow() {
        Assert.fail("not yet written");
    }
    
    //@Test
    public void testPostInvalidColumnName() {
        Assert.fail("not yet written");
    }
    
    @Test
    public void testLoadCsvData() {
        try {
            String testTable = testSchemaName + ".testLoadData";
            TableDesc orig = new TableDesc(testSchemaName, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c1", TapDataType.SHORT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c2", TapDataType.INTEGER));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c3", TapDataType.LONG));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c4", TapDataType.FLOAT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c5", TapDataType.DOUBLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c6", TapDataType.TIMESTAMP));
            
            TableCreator tc = new TableCreator(dataSource);
            try {
                tc.dropTable(testTable);
            } catch (Exception ignore) {
            }
            log.info("createTable...");
            tc.createTable(orig);
            log.info("createTable... [OK]");
            
            StringBuilder csvData = new StringBuilder();
            csvData.append("c0, c6, c2\n");
            csvData.append("string0,2018-11-05T22:12:33.111,0\n");
            csvData.append("string1,2018-11-05T22:12:33.111,1\n");
            csvData.append("string2,2018-11-05T22:12:33.111,2\n");
            csvData.append("string3,2018-11-05T22:12:33.111,3\n");
            csvData.append("string4,2018-11-05T22:12:33.111,4\n");
            csvData.append("string5,2018-11-05T22:12:33.111,5\n");
            csvData.append("string6,2018-11-05T22:12:33.111,6\n");
            csvData.append("string7,2018-11-05T22:12:33.111,7\n");
            csvData.append("string8,2018-11-05T22:12:33.111,8\n");
            csvData.append("string9,2018-11-05T22:12:33.111,9\n");
            
            AsciiTableData tw = new AsciiTableData(new ByteArrayInputStream(csvData.toString().getBytes()), "text/csv");
            TableLoader tableLoader = new TableLoader(dataSource, 3);
            log.info("load...");
            tableLoader.load(orig, tw);
            log.info("load... [OK]");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("count", rows.size() == 10);
            
            // cleanup
            tc.dropTable(testTable);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
