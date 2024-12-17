package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.parquet.ParquetReader.TableShape;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
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
            ParquetReader reader = new ParquetReader();
            InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
            TableShape readerResponse = reader.read(inputStream);

            log.info(readerResponse.getColumnCount() + " columns, " + readerResponse.getRecordCount() + " records");

            Assert.assertTrue(readerResponse.getRecordCount() > 0);
            Assert.assertTrue(readerResponse.getColumnCount() > 0);
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail("unexpected exception: " + e);
        }
    }
}
