package org.opencadc.youcat;

import ca.nrc.cadc.conformance.uws2.JobResultWrapper;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.tap.integration.TapSyncUploadTest;
import ca.nrc.cadc.tap.integration.VOTableHandler;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
// NOTE: disabled until we support UPLOAD in cadc-rest based UWS async endpoint
public class CatSyncUploadTest extends TapSyncUploadTest {
    private static final Logger log = Logger.getLogger(CatSyncUploadTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
    }

    public CatSyncUploadTest() {
        super(Constants.RESOURCE_ID);
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", CatSyncUploadTest.class);
        setTestFile(f);
        setTestURL(CatAsyncUploadTest.getVOTableURL(f));
    }
    
    @Test
    public void testUploadBinary() {
        try {
            Log4jInit.setLevel("org.opencadc.youcat", Level.DEBUG);
            
            File binFile = FileUtil.getFileFromResource("binary-vot.xml", CatSyncUploadTest.class);
            
            String tableName = "mytab";
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("upload1", binFile);
            params.put("UPLOAD", tableName + ",param:upload1");
            params.put("LANG", "ADQL");
            params.put("QUERY", "select * from tap_upload." + tableName);

            JobResultWrapper result = createAndExecuteSyncParamJobPOST("testUploadFile", params);

            Assert.assertNull(result.throwable);
            Assert.assertEquals(200, result.responseCode);

            Assert.assertNotNull(result.syncOutput);
            ByteArrayInputStream istream = new ByteArrayInputStream(result.syncOutput);
            VOTableReader vrdr = new VOTableReader();
            VOTableDocument vot = vrdr.read(istream);

            String queryStatus = getQueryStatus(vot);
            Assert.assertNotNull("QUERY_STATUS", queryStatus);
            Assert.assertEquals("OK", queryStatus);

            // TODO: verify round-trip of testFile1 -> vot?
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    static String getQueryStatus(VOTableDocument vot) {
        VOTableResource vr = vot.getResourceByType("results");
        Assert.assertNotNull(vr);
        log.debug("found resource: " + vr.getName() + " " + vr.getType());
        String ret = null;
        // find the last QUERY_STATUS and return that because there can be a trailing
        // status when result processing fails
        for (VOTableInfo vi : vr.getInfos()) {
            if ("QUERY_STATUS".equals(vi.getName())) {
                ret = vi.getValue();
                log.warn("found status: " + ret);
            }
        }
        log.warn("return status: " + ret);
        return ret;
    }
}
