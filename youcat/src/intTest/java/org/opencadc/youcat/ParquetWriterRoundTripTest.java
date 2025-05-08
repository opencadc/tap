package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.parquet.ParquetReader.TableShape;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.vosi.actions.TableContentHandler;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

public class ParquetWriterRoundTripTest extends AbstractTablesTest {
    private static final Logger log = Logger.getLogger(ParquetWriterRoundTripTest.class);
    private static URL url;
    private static final Charset UTF8 = Charset.forName("utf-8");

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
        VOTableTable voTableTable = getVOTableFromVOTableWriter(testTable);

        // get votable from parquet file
        VOTableTable voTableFromParquet = getVOTableFromParquet(testTable);

        compareVOTables(voTableTable, voTableFromParquet);

        doDelete(schemaOwner, testTable, false);
    }

    private static VOTableTable extractVOTableFromOutputStream(ByteArrayOutputStream out, String adql) throws IOException {
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        TableShape readerResponse = reader.read(inputStream);

        log.info(readerResponse.getColumnCount() + " columns, " + readerResponse.getRecordCount() + " records");

        Assert.assertTrue(readerResponse.getRecordCount() > 0);
        Assert.assertTrue(readerResponse.getColumnCount() > 0);

        VOTableDocument voTableDocument = readerResponse.getVoTableDocument();

        Assert.assertNotNull(voTableDocument.getResources());

        VOTableResource results = voTableDocument.getResourceByType("results");
        Assert.assertNotNull(results);

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
        Assert.assertEquals(readerResponse.getColumnCount(), results.getTable().getFields().size());
        return results.getTable();
    }

    private void createCustomTable(String testTable) throws Exception {
        clearSchemaPerms();

        TapPermissions tapPermissions = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tapPermissions, 200);

        doCreateTable(schemaOwner, testTable);
    }

    private void addDataToCustomTable(String testTable) throws MalformedURLException, PrivilegedActionException {
        StringBuilder data = prepareData();

        URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
        final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
        Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                post.run();
                return null;
            }
        });

        Assert.assertNull(post.getThrowable());
        Assert.assertEquals(200, post.getResponseCode());
    }

    private static void compareVOTables(VOTableTable voTableFromVOTableWriter, VOTableTable voTableFromParquet) {
        Assert.assertEquals(voTableFromVOTableWriter.getFields().size(), voTableFromParquet.getFields().size());
        Assert.assertEquals(voTableFromVOTableWriter.getInfos().size(), voTableFromParquet.getInfos().size());

        // compare the fields
        for (int i = 0; i < voTableFromVOTableWriter.getFields().size(); i++) {
            VOTableField field1 = voTableFromVOTableWriter.getFields().get(i);
            VOTableField field2 = voTableFromParquet.getFields().get(i);

            Assert.assertEquals(field1.getName(), field2.getName());

            if (field1.xtype != null && field1.xtype.equals("timestamp")) {
                Assert.assertEquals("long", field2.getDatatype());

                Assert.assertNull(field2.xtype);
                Assert.assertNull(field2.getArraysize());
            } else if (field1.getDatatype().equals("short")) {
                Assert.assertEquals("int", field2.getDatatype());
            } else {
                Assert.assertEquals(field1.getDatatype(), field2.getDatatype());
                Assert.assertEquals(field1.getArraysize(), field2.getArraysize());
            }
        }
    }

    private String queryVOTableWriter(String testTable) throws Exception {
        String adql = "SELECT * from " + testTable;

        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "votable");

        String result = Subject.doAs(schemaOwner, new AuthQueryTest.SyncQueryAction(anonQueryURL, params));
        Assert.assertNotNull(result);

        return result;
    }

    private VOTableTable getVOTableFromVOTableWriter(String testTable) throws Exception {
        String result = queryVOTableWriter(testTable);

        VOTableReader voTableReader = new VOTableReader();
        VOTableDocument voTableDocument = voTableReader.read(result);
        VOTableResource voTableResource = voTableDocument.getResourceByType("results");
        VOTableTable voTable = voTableResource.getTable();

        Assert.assertNotNull(voTable);
        Assert.assertNotNull(voTable.getTableData());

        return voTable;
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

    private static StringBuilder prepareData() {
        StringBuilder data = new StringBuilder();
        data.append("c0\tc1\tc2\tc3\tc4\tc5\tc6\te7\te8\te9\te10\ta11\ta12\ta13\ta14\ta15\n");
        for (int i = 0; i < 10; i++) {
            data.append("string" + i).append("\t");
            data.append(Short.MAX_VALUE).append("\t");
            data.append(Integer.MAX_VALUE).append("\t");
            data.append(Long.MAX_VALUE).append("\t");
            data.append(Float.MAX_VALUE).append("\t");
            data.append(Double.MAX_VALUE).append("\t");

            data.append("2018-11-05T22:12:33.111").append("\t");

            data.append("1.0 2.0").append("\t");  // interval
            data.append("1.0 2.0").append("\t");  // point
            data.append("1.0 2.0 3.0").append("\t");  // circle
            data.append("1.0 2.0 3.0 4.0 5.0 6.0").append("\t");  // polygon

            data.append(Short.MIN_VALUE + " " + Short.MAX_VALUE).append("\t");
            data.append(Integer.MIN_VALUE + " " + Integer.MAX_VALUE).append("\t");
            data.append(Long.MIN_VALUE + " " + Long.MAX_VALUE).append("\t");
            data.append(Float.MIN_VALUE + " " + Float.MAX_VALUE).append("\t");
            data.append(Double.MIN_VALUE + " " + Double.MAX_VALUE).append("\n");
        }
        return data;
    }
}
