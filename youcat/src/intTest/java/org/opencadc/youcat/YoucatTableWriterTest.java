package org.opencadc.youcat;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.OutputStreamWrapper;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.vosi.TableWriter;
import ca.nrc.cadc.vosi.actions.TablesInputHandler;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class YoucatTableWriterTest extends AbstractTablesTest {

    private static final Logger log = Logger.getLogger(YoucatTableWriterTest.class);

    @Test
    public void testYoucatTableWriter() throws Exception {
        String tableName = testSchemaName + ".testYoucatWriter";

        createServiceDescriptorTemplates();
        createCustomTable(tableName);

        String query = "SELECT * FROM " + tableName;
        List<VOTableResource> resources = executeQueryAndGetResources(query);
        verifyIDFieldsCount(resources, 3); // c0, c1, c2 are ID fields
        List<VOTableResource> metaResources = resources.stream().filter(resource -> resource.getType().equals("meta")).collect(Collectors.toList());
        Assert.assertEquals(2, metaResources.size());
        Assert.assertEquals(2, metaResources.stream().filter(e -> e.getName().equals("testServiceDescriptorTemplate1") || e.getName().equals("testServiceDescriptorTemplate2")).count());

        query = "SELECT c1, c2, c3, c4, c5 FROM " + tableName;
        resources = executeQueryAndGetResources(query);
        verifyIDFieldsCount(resources, 2); // c1, c2 are ID fields
        metaResources = resources.stream().filter(resource -> resource.getType().equals("meta")).collect(Collectors.toList());
        Assert.assertEquals(1, metaResources.size());
        Assert.assertEquals("testServiceDescriptorTemplate2", metaResources.get(0).getName());

        query = "SELECT c2, c3, c4, c5 FROM " + tableName;
        resources = executeQueryAndGetResources(query);
        verifyIDFieldsCount(resources, 1); // c2 is an ID field
        metaResources = resources.stream().filter(resource -> resource.getType().equals("meta")).collect(Collectors.toList());
        Assert.assertEquals(0, metaResources.size());

        query = "SELECT c3, c4, c5 from " + tableName;
        resources = executeQueryAndGetResources(query);
        verifyIDFieldsCount(resources, 0); // no ID fields
        metaResources = resources.stream().filter(resource -> resource.getType().equals("meta")).collect(Collectors.toList());
        Assert.assertEquals(0, metaResources.size());
    }

    private static void verifyIDFieldsCount(List<VOTableResource> resources, long expectedCount) {
        long fieldsWithIDCount = resources.stream()
                .filter(resource -> resource.getTable() != null)
                .flatMap(resource -> resource.getTable().getFields().stream())
                .filter(field -> field.id != null)
                .count();

        Assert.assertEquals(expectedCount, fieldsWithIDCount);
    }

    private void createCustomTable(String tableName) throws Exception {
        clearSchemaPerms();

        TapPermissions tapPermissions = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tapPermissions, 200);

        TableDesc orig = prepareTable(tableName);
        uploadTable(tableName, orig);

        log.info("Table " + tableName + " created successfully.");
    }

    private void createServiceDescriptorTemplates() throws IOException, ResourceNotFoundException {
        uploadServiceDescriptorTemplate("service-descriptor-template-1.xml", "testServiceDescriptorTemplate1");
        uploadServiceDescriptorTemplate("service-descriptor-template-2.xml", "testServiceDescriptorTemplate2");

        log.info("Service descriptor templates created successfully.");
    }

    private void uploadServiceDescriptorTemplate(String sdFileName, String sdName) throws IOException, ResourceNotFoundException {
        File testFile = FileUtil.getFileFromResource(sdFileName, YoucatTableWriterTest.class);
        String template = Files.readString(testFile.toPath());

        JSONObject json = new JSONObject();
        json.put("name", sdName);
        json.put("template", template);

        FileContent content = new FileContent(json.toString().getBytes(StandardCharsets.UTF_8), "application/json");
        /*HttpPost httpPost = new HttpPost(templatesURL, content, false); // TODO: use the actual URL for templates when available
        httpPost.run();

        Assert.assertNull(httpPost.getThrowable());
        Assert.assertEquals(200, httpPost.getResponseCode());*/
    }

    private void uploadTable(String tableName, TableDesc orig) throws Exception {
        URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + tableName);
        TableWriter w = new TableWriter();
        StringWriter sw = new StringWriter();
        w.write(orig, sw);
        log.info("VOSI-table description:\n" + sw.toString());

        OutputStreamWrapper src = new OutputStreamWrapper() {
            @Override
            public void write(OutputStream out) throws IOException {
                TableWriter w = new TableWriter();
                w.write(orig, new OutputStreamWriter(out));
            }
        };
        HttpUpload put = new HttpUpload(src, tableURL);
        put.setContentType(TablesInputHandler.VOSI_TABLE_TYPE);
        log.info("doCreateTable: " + tableURL);
        Subject.doAs(schemaOwner, new RunnableAction(put));
        log.info("doCreateTable: " + put.getResponseCode());
        if (put.getThrowable() != null && put.getThrowable() instanceof Exception) {
            throw (Exception) put.getThrowable();
        }
        Assert.assertEquals("response code", 200, put.getResponseCode());
    }

    private TableDesc prepareTable(String tableName) {
        final TableDesc orig = new TableDesc(testSchemaName, tableName);
        orig.description = "created by intTest";
        orig.tableType = TableDesc.TableType.TABLE;
        orig.tableIndex = 1;

        ColumnDesc c0 = new ColumnDesc(tableName, "c0", TapDataType.STRING);
        c0.columnID = "testServiceDescriptorID1";
        orig.getColumnDescs().add(c0);

        ColumnDesc c1 = new ColumnDesc(tableName, "c1", TapDataType.SHORT);
        c1.columnID = "testServiceDescriptorID2";
        orig.getColumnDescs().add(c1);

        ColumnDesc c2 = new ColumnDesc(tableName, "c2", TapDataType.INTEGER);
        c2.columnID = "testServiceDescriptorID3";
        orig.getColumnDescs().add(c2);

        orig.getColumnDescs().add(new ColumnDesc(tableName, "c3", TapDataType.INTEGER));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c4", TapDataType.INTEGER));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c5", TapDataType.INTEGER));
        return orig;
    }

    private List<VOTableResource> executeQueryAndGetResources(String query) throws Exception {
        String result = doQuery(query);
        VOTableReader r = new VOTableReader();
        VOTableDocument doc = r.read(result);

        return doc.getResources();
    }

    private String doQuery(String query) throws Exception {
        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", query);
        String result = Subject.doAs(anon, new AuthQueryTest.SyncQueryAction(anonQueryURL, params));
        Assert.assertNotNull(result);
        return result;
    }
}