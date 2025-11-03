package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.parquet.ParquetWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.io.RandomAccessFile;
import ca.nrc.cadc.io.RandomAccessSource;
import ca.nrc.cadc.net.RandomAccessURL;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.tap.schema.TapPermissions;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class ParquetReaderTest extends AbstractTablesTest {

    private static final Logger log = Logger.getLogger(ParquetReaderTest.class);

    public ParquetReaderTest() {
        super();
    }

    @Test
    public void testParquetReader() throws Exception {
        log.info("Running ParquetReaderTest");
        testParquetReader(true);
        testParquetReader(false);
        testRemoteAccessURL(true);
        testRemoteAccessURL(false);
    }

    private void testParquetReader(boolean withMetadata) throws Exception {
        String testTable = testSchemaName + ".testReadParquet";

        // create table
        clearSchemaPerms();

        TapPermissions tapPermissions = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tapPermissions, 200);

        doCreateTable(schemaOwner, testTable);
        setPerms(schemaOwner, testTable, tapPermissions, 200);

        // load table data
        loadTableData(testTable);

        // Write out the table into Parquet format
        ByteArrayOutputStream out = getParquetStreamingData(withMetadata, testTable);

        // Read the Parquet data into a VOTableDocument
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        try (VOTableDocument voDocFromParquetReader = reader.read(inputStream)) {
            Assert.assertNotNull(voDocFromParquetReader);
            verifyAllDataTypes(voDocFromParquetReader.getResourceByType("results").getTable(), true, !withMetadata);
        }

        // Test local parquet file via FileRandomAccessSource
        File tempFile = File.createTempFile("parquetTest", ".parquet");
        tempFile.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(out.toByteArray());
        }

        RandomAccessSource randomAccessSource = new RandomAccessFile(tempFile, "r");
        try (VOTableDocument voDocFromParquetReader = reader.read(randomAccessSource)) {
            Assert.assertNotNull(voDocFromParquetReader);
            verifyAllDataTypes(voDocFromParquetReader.getResourceByType("results").getTable(), true, !withMetadata);
        }
    }

    private void testRemoteAccessURL(boolean withMetadata) throws IOException, ResourceNotFoundException, URISyntaxException {
        ParquetReader reader = new ParquetReader();
        String remoteFileName = withMetadata ? "parquet-with-metadata" : "parquet-without-metadata";
        URL artifactURL = new URL("https://ws-cadc.canfar.net/vault/files/CADC/test-data/tap-upload/" + remoteFileName + ".parquet");

        RandomAccessSource randomAccessSource = new RandomAccessURL(artifactURL);
        try (VOTableDocument voDocFromParquetReader = reader.read(randomAccessSource)) {
            Assert.assertNotNull(voDocFromParquetReader);
            verifyAllDataTypes(voDocFromParquetReader.getResourceByType("results").getTable(), true, !withMetadata);
        }
    }

    // Write out the table into Parquet format - Use writer directly to test it with and without metadata
    private ByteArrayOutputStream getParquetStreamingData(boolean withMetadata, String testTable) throws Exception {
        // get data in votable format
        String result = doQuery(testTable);
        Assert.assertNotNull(result);

        VOTableReader voTableReader = new VOTableReader();
        VOTableDocument voTableDocument = voTableReader.read(result);

        // write the votable to parquet format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetWriter parquetWriter = new ParquetWriter(withMetadata);
        parquetWriter.write(voTableDocument, out);
        return out;
    }

    private void loadTableData(String testTable) throws Exception {
        String data = doPrepareDataAllDataTypesTSV();

        doUploadTSVData(testTable, data);
    }

}