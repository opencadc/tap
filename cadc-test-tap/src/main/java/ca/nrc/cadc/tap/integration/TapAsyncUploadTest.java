
package ca.nrc.cadc.tap.integration;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.conformance.uws2.AbstractUWSTest2;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.ParameterUtil;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.server.StringIDGenerator;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class TapAsyncUploadTest extends AbstractUWSTest2 {

    private static final Logger log = Logger.getLogger(TapAsyncUploadTest.class);

    private File testFile;
    private URL testURL;
    private final StringIDGenerator idgen = new RandomStringGenerator(8);

    public TapAsyncUploadTest(URI resourceID) {
        super(resourceID, Standards.TAP_10, Standards.INTERFACE_UWS_ASYNC);
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

            URL jobURL = createAsyncParamJob("testUploadFile", params);

            Assert.assertNotNull(jobURL);

            JobReader jr = new JobReader();
            Job job = jr.read(jobURL.openStream());
            List<Parameter> jparams = job.getParameterList();
            String upv = ParameterUtil.findParameterValue("UPLOAD", jparams);
            Assert.assertNotNull(upv);
            String[] parts = upv.split(",");
            Assert.assertEquals(2, parts.length);
            Assert.assertEquals(tableName, parts[0]);
            URI uri = new URI(parts[1]);
            Assert.assertNotNull(uri);

            URL tmpURL = uri.toURL();  // tmp storage of inline upload

            // for async we can verify that the file was uploaded intact
            VOTableDocument vot = VOTableHandler.getVOTable(tmpURL);
            log.info("testUploadFile: found valid VOTable: " + tmpURL);

            // TODO: compare testFile1 vs vot (inline upload)?
            // TODO: run the job, get the output votable, and compare it to testFile1 (roundtrip)?
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

            URL jobURL = createAsyncParamJob("testUploadFile", params);

            Assert.assertNotNull(jobURL);

            JobReader jr = new JobReader();
            Job job = jr.read(jobURL.openStream());
            List<Parameter> jparams = job.getParameterList();
            String upv = ParameterUtil.findParameterValue("UPLOAD", jparams);
            Assert.assertNotNull(upv);
            String[] parts = upv.split(",");
            Assert.assertEquals(2, parts.length);
            Assert.assertEquals(tableName, parts[0]);
            URL tmpURL = new URL(parts[1]);
            Assert.assertEquals(testURL, tmpURL);
            log.info("testUploadURL: VOTable: " + tmpURL);

            // TODO: run the job, get the output votable, and compare it to testFile1 (inline upload)?
            // TODO: run the job, get the output votable, and compare it to testFile1 (roundtrip)?
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
