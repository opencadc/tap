package org.opencadc.youcat;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.parquet.ParquetWriter;
import ca.nrc.cadc.dali.tables.parquet.io.FileRandomAccessSource;
import ca.nrc.cadc.dali.tables.parquet.io.HttpRandomAccessSource;
import ca.nrc.cadc.dali.tables.parquet.io.InMemoryRandomAccessSource;
import ca.nrc.cadc.dali.tables.parquet.io.RandomAccessSource;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.vosi.actions.TableContentHandler;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

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
        testHttpRemoteAccessSource(true);
        testHttpRemoteAccessSource(false);
    }

    private void testParquetReader(boolean withMetadata) throws Exception {
        String testTable = testSchemaName + ".testReadParquet";

        // create table
        doCreateTable(schemaOwner, testTable);

        // load table data
        loadTableData(testTable);

        // Write out the table into Parquet format
        ByteArrayOutputStream out = getParquetStreamingData(withMetadata, testTable);

        // Read the Parquet data into a VOTableDocument
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        VOTableDocument voDocFromParquetReader = reader.read(inputStream);
        Assert.assertNotNull(voDocFromParquetReader);

        verifyTableData(voDocFromParquetReader.getResourceByType("results").getTable(), withMetadata);

        // Test In memory parquet content via InMemoryRandomAccessSource
        RandomAccessSource randomAccessSource = new InMemoryRandomAccessSource(out.toByteArray());
        voDocFromParquetReader = reader.read(randomAccessSource);
        Assert.assertNotNull(voDocFromParquetReader);
        verifyTableData(voDocFromParquetReader.getResourceByType("results").getTable(), withMetadata);

        // Test local parquet file via FileRandomAccessSource
        File tempFile = File.createTempFile("parquetTest", ".parquet");
        tempFile.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(out.toByteArray());
        }

        randomAccessSource = new FileRandomAccessSource(tempFile);
        voDocFromParquetReader = reader.read(randomAccessSource);
        Assert.assertNotNull(voDocFromParquetReader);
        verifyTableData(voDocFromParquetReader.getResourceByType("results").getTable(), withMetadata);
    }

    private void testHttpRemoteAccessSource(boolean withMetadata) throws IOException, ResourceNotFoundException {
        ParquetReader reader = new ParquetReader();
        String remoteFileName = withMetadata ? "parquet-with-metadata" : "parquet-without-metadata";
        URL artifactURL = new URL("https://ws-cadc.canfar.net/vault/files/CADC/test-data/tap-upload/" + remoteFileName + ".parquet");

        RandomAccessSource randomAccessSource = new HttpRandomAccessSource(artifactURL);
        VOTableDocument voDocFromParquetReader = reader.read(randomAccessSource);
        Assert.assertNotNull(voDocFromParquetReader);
        verifyTableData(voDocFromParquetReader.getResourceByType("results").getTable(), withMetadata);
    }

    // Write out the table into Parquet format - Use writer directly to test it with and without metadata
    private ByteArrayOutputStream getParquetStreamingData(boolean withMetadata, String testTable) throws PrivilegedActionException, IOException {
        // get data in votable format
        String adql = "SELECT * from " + testTable;

        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "votable");

        String result = Subject.doAs(schemaOwner, new AuthQueryTest.SyncQueryAction(anonQueryURL, params));
        Assert.assertNotNull(result);

        VOTableReader voTableReader = new VOTableReader();
        VOTableDocument voTableDocument = voTableReader.read(result);

        // write the votable to parquet format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetWriter parquetWriter = new ParquetWriter(withMetadata);
        parquetWriter.write(voTableDocument, out);
        return out;
    }

    private void verifyTableData(VOTableTable voTableFromParquet, boolean withMetadata) {
        Iterator<List<Object>> parquetDataIterator = voTableFromParquet.getTableData().iterator();
        int count = 0;
        while (parquetDataIterator.hasNext()) {
            List<Object> next = parquetDataIterator.next();
            Assert.assertEquals("string" + count, next.get(0));
            Assert.assertEquals((int) Short.MAX_VALUE, next.get(1));
            Assert.assertEquals(Integer.MAX_VALUE, next.get(2));
            Assert.assertEquals(Long.MAX_VALUE, next.get(3));
            Assert.assertEquals(Float.MAX_VALUE, next.get(4));
            Assert.assertEquals(Double.MAX_VALUE, next.get(5));
            Assert.assertTrue(next.get(6) instanceof Date);
            if (withMetadata) {
                assertEquals(new DoubleInterval(1.0, 2.0), (DoubleInterval) next.get(7));
                assertEquals(new Point(1.0, 2.0), (Point) next.get(8));
                assertEquals(new Circle(new Point(1, 2), 3), (Circle) next.get(9));
                Polygon p = new Polygon();
                p.getVertices().add(new Point(1.0, 2.0));
                p.getVertices().add(new Point(3.0, 4.0));
                p.getVertices().add(new Point(5.0, 6.0));
                assertEquals(p, (Polygon) next.get(10));
            } else {
                Assert.assertArrayEquals(new double[]{1.0, 2.0}, (double[]) next.get(7), 0.0001);
                Assert.assertArrayEquals(new double[]{1.0, 2.0}, (double[]) next.get(8), 0.0001);
                Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0}, (double[]) next.get(9), 0.0001);
                Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, (double[]) next.get(10), 0.0001);
            }

            Assert.assertArrayEquals(new int[]{Short.MIN_VALUE, Short.MAX_VALUE}, (int[]) next.get(11));
            Assert.assertArrayEquals(new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}, (int[]) next.get(12));
            Assert.assertArrayEquals(new long[]{Long.MIN_VALUE, Long.MAX_VALUE}, (long[]) next.get(13));
            Assert.assertArrayEquals(new float[]{Float.MIN_VALUE, Float.MAX_VALUE}, (float[]) next.get(14), 0.0001f);
            Assert.assertArrayEquals(new double[]{Double.MIN_VALUE, Double.MAX_VALUE}, (double[]) next.get(15), 0.0001);

            Assert.assertEquals("ivo://opencadc.org/youcat", next.get(16).toString());
            Assert.assertTrue(next.get(17) instanceof UUID);
            count++;
        }
        Assert.assertEquals(10, count);
    }

    private void loadTableData(String testTable) throws Exception {
        StringBuilder data = prepareData();

        URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
        final HttpPost post = new HttpPost(postURL, data.toString(), TableContentHandler.CONTENT_TYPE_TSV, false);
        Subject.doAs(schemaOwner, (PrivilegedExceptionAction<Object>) () -> {
            post.run();
            return null;
        });

        Assert.assertNull(post.getThrowable());
        Assert.assertEquals(200, post.getResponseCode());
    }

    private static StringBuilder prepareData() {
        StringBuilder data = new StringBuilder();
        data.append("c0\tc1\tc2\tc3\tc4\tc5\tc6\te7\te8\te9\te10\ta11\ta12\ta13\ta14\ta15\te16\te17\n");
        for (int i = 0; i < 10; i++) {
            data.append("string").append(i).append("\t");
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
            data.append(Double.MIN_VALUE + " " + Double.MAX_VALUE).append("\t");

            data.append(URI.create("ivo://opencadc.org/youcat")).append("\t");
            data.append(UUID.randomUUID()).append("\n");
        }
        return data;
    }

}
