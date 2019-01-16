
package ca.nrc.cadc.tap.integration;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.conformance.uws2.AbstractUWSTest2;
import ca.nrc.cadc.conformance.uws2.JobResultWrapper;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.server.StringIDGenerator;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class TapSyncUploadTest extends AbstractUWSTest2 {

    private static final Logger log = Logger.getLogger(TapSyncUploadTest.class);

    private File testFile;
    private URL testURL;

    private final StringIDGenerator idgen = new RandomStringGenerator(8);

    public TapSyncUploadTest(URI resourceID) {
        super(resourceID, Standards.TAP_10, Standards.INTERFACE_UWS_SYNC);
    }

    public void setTestFile(File testFile) {
        this.testFile = testFile;
    }

    public void setTestURL(URL testURL) {
        this.testURL = testURL;
    }

    @Test
    public void testUploadFile() {
        try {
            String tableName = "mytab";
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("upload1", testFile);
            params.put("UPLOAD", tableName + ",param:upload1");
            params.put("LANG", "ADQL");
            params.put("QUERY", "select * from tap_upload." + tableName);

            JobResultWrapper result = createAndExecuteSyncParamJobPOST("testUploadFile", params);

            Assert.assertNull(result.throwable);
            Assert.assertEquals(200, result.responseCode);

            Assert.assertNotNull(result.syncOutput);
            VOTableDocument vot = VOTableHandler.getVOTable(result.syncOutput);

            String queryStatus = VOTableHandler.getQueryStatus(vot);
            Assert.assertNotNull("QUERY_STATUS", queryStatus);
            Assert.assertEquals("OK", queryStatus);

            // TODO: verify round-trip of testFile1 -> vot?
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUploadURL() {
        try {
            String tableName = "tab_" + idgen.getID(); // put to WEBTMP

            Map<String, Object> params = new HashMap<String, Object>();
            params.put("UPLOAD", tableName + "," + testURL.toExternalForm());
            params.put("LANG", "ADQL");
            params.put("QUERY", "select * from tap_upload." + tableName);

            JobResultWrapper result = createAndExecuteSyncParamJobPOST("testUploadURL", params);

            Assert.assertNull(result.throwable);
            Assert.assertEquals(200, result.responseCode);

            Assert.assertNotNull(result.syncOutput);
            VOTableDocument vot = VOTableHandler.getVOTable(result.syncOutput);

            String queryStatus = VOTableHandler.getQueryStatus(vot);
            Assert.assertNotNull("QUERY_STATUS", queryStatus);
            Assert.assertEquals("OK", queryStatus);

            // TODO: verify round-trip of testFile1 -> vot?
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
