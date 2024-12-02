package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

public class ParquetWriterRoundTripTest {
    private static final Logger log = Logger.getLogger(ParquetWriterRoundTripTest.class);
    private static URL url;

    @Test
    public void testWriteParquet() throws Exception {
        String adql = "SELECT * from tap_schema.columns";

        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "parquet");

        TapClient tapClient = new TapClient(Constants.RESOURCE_ID);
        url = tapClient.getSyncURL(Standards.SECURITY_METHOD_ANON);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpPost httpPost = new HttpPost(url, params, out);
        httpPost.prepare();

        if (httpPost.getThrowable() != null) {
            log.error("Post failed", httpPost.getThrowable());
            Assert.fail("exception on post: " + httpPost.getThrowable());
        }

        int code = httpPost.getResponseCode();
        Assert.assertEquals(200, code);

        String contentType = httpPost.getContentType();
        Assert.assertEquals("application/vnd.apache.parquet", contentType);
        String writerResponse = httpPost.getResponseBody();

        Path filePath = Path.of("/tmp/pqop.parquet");
        try {
            // Write bytes to the file
            Files.write(filePath, writerResponse.getBytes());
            log.info("filepath: " + filePath.toAbsolutePath());

            ParquetReader reader = new ParquetReader();
            String readerResponse = reader.read("/tmp/pqop.parquet");
            Assert.assertEquals("Worked", readerResponse);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
