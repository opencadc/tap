package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.tap.schema.TapPermissions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

public class ParquetWriterTest extends AbstractTablesTest {
    private static final Logger log = Logger.getLogger(ParquetWriterTest.class);
    private static URL url;

    @Test
    public void testWriteParquetWithExistingTable() throws Exception {
        String adql = "SELECT * from tap_schema.columns";

        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "parquet");

        TapClient tapClient = new TapClient(Constants.RESOURCE_ID);
        url = tapClient.getSyncURL(Standards.SECURITY_METHOD_ANON);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpPost httpPost = new HttpPost(url, params, out);
        httpPost.run();

        if (httpPost.getThrowable() != null) {
            log.error("Post failed", httpPost.getThrowable());
            Assert.fail("exception on post: " + httpPost.getThrowable());
        }

        int code = httpPost.getResponseCode();
        Assert.assertEquals(200, code);

        String contentType = httpPost.getContentType();
        Assert.assertEquals("application/vnd.apache.parquet", contentType);

        try {
            extractVOTableFromOutputStream(out, adql);
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testWriteParquetWithCustomTable() throws Exception {

        String testTable = testSchemaName + ".testWriteParquet";

        createCustomTable(testTable);

        addDataToCustomTable(testTable);

        // get votable from votable query
        VOTableTable voTableTable = doQueryForVOT(testTable);

        // get votable from parquet file
        VOTableTable voTableFromParquet = getVOTableFromParquet(testTable);

        compareVOTables(voTableTable, voTableFromParquet);

        doDelete(schemaOwner, testTable, false);
    }

    private static VOTableTable extractVOTableFromOutputStream(ByteArrayOutputStream out, String adql) throws IOException {
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        VOTableDocument voTableDocument = reader.read(inputStream);
        Assert.assertNotNull(voTableDocument.getResources());

        VOTableResource results = voTableDocument.getResourceByType("results");
        Assert.assertNotNull(results);
        log.info(results.getTable().getFields().size() + " columns found.");
        Assert.assertFalse(results.getTable().getFields().isEmpty());

        boolean queryFound = false;
        boolean queryStatusFound = false;

        for (VOTableInfo voTableInfo : results.getInfos()) {
            if (voTableInfo.getName().equals("QUERY")) {
                queryFound = true;
                Assert.assertEquals(adql, voTableInfo.getValue());
            } else if (voTableInfo.getName().equals("QUERY_STATUS")) {
                queryStatusFound = true;
                Assert.assertEquals("OK", voTableInfo.getValue());
            }
        }

        Assert.assertTrue(queryFound);
        Assert.assertTrue(queryStatusFound);

        Assert.assertNotNull(results.getTable());
        return results.getTable();
    }

    private void createCustomTable(String testTable) throws Exception {
        clearSchemaPerms();

        TapPermissions tapPermissions = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tapPermissions, 200);

        doCreateTable(schemaOwner, testTable);
        setPerms(schemaOwner, testTable, tapPermissions, 200);
    }

    private void addDataToCustomTable(String testTable) throws MalformedURLException, PrivilegedActionException, URISyntaxException {
        String data = doPrepareDataAllDataTypesTSV();

        doUploadTSVData(testTable, data);
    }

    private void compareVOTables(VOTableTable voTableFromVOTableWriter, VOTableTable voTableFromParquet) throws IOException, URISyntaxException {
        Assert.assertEquals(voTableFromVOTableWriter.getFields().size(), voTableFromParquet.getFields().size());
        Assert.assertEquals(voTableFromVOTableWriter.getInfos().size(), voTableFromParquet.getInfos().size());

        // compare the fields
        for (int i = 0; i < voTableFromVOTableWriter.getFields().size(); i++) {
            VOTableField field1 = voTableFromVOTableWriter.getFields().get(i);
            VOTableField field2 = voTableFromParquet.getFields().get(i);

            Assert.assertEquals(field1.getName(), field2.getName());

            if (field1.getDatatype().equals("short")) {
                Assert.assertEquals("int", field2.getDatatype());
            } else {
                Assert.assertEquals(field1.getDatatype(), field2.getDatatype());
                Assert.assertEquals(field1.getArraysize(), field2.getArraysize());
            }
        }

        verifyAllDataTypes(voTableFromParquet,true, false);
    }

    private VOTableTable getVOTableFromParquet(String testTable) throws Exception {
        String adql = "SELECT * from " + testTable;

        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "parquet");

        TapClient tapClient = new TapClient(Constants.RESOURCE_ID);
        url = tapClient.getSyncURL(Standards.SECURITY_METHOD_ANON);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String result = Subject.doAs(schemaOwner, new AuthQueryTest.SyncQueryAction(anonQueryURL, params, out, "application/vnd.apache.parquet"));
        Assert.assertNotNull(result);

        return extractVOTableFromOutputStream(out, adql);
    }

}
