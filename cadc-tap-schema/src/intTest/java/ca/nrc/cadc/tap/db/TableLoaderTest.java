package ca.nrc.cadc.tap.db;

import java.io.ByteArrayInputStream;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.tap.db.TableCreatorTest.SimpleRowMapper;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.util.Log4jInit;

public class TableLoaderTest {

    private static final Logger log = Logger.getLogger(TableLoaderTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.db", Level.INFO);
    }
    
    private DataSource dataSource;
    private final String TEST_SCHEMA = "tap_schema";
    
    public TableLoaderTest() { 
        // create a datasource and register with JNDI
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("TAP_SCHEMA_TEST", "cadctest");
            dataSource = DBUtil.getDataSource(cc);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
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
            String testTable = TEST_SCHEMA + ".testLoadData";
            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c1", TapDataType.SHORT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c2", TapDataType.INTEGER));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c3", TapDataType.LONG));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c4", TapDataType.FLOAT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c5", TapDataType.DOUBLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c6", TapDataType.TIMESTAMP));
            
            TableCreator tc = new TableCreator(dataSource);
            tc.createTable(orig);
            log.info("createTable returned");
            
            TableLoader tableLoader = new TableLoader(dataSource, 3);
            
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
            
            AsciiTableData tw = new AsciiTableData(
                new ByteArrayInputStream(csvData.toString().getBytes()),
                "text/csv");
            tableLoader.load(orig, tw);
            
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
