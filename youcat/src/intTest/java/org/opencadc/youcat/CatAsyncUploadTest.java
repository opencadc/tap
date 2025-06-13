
package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.integration.TapAsyncUploadTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Parameter;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class CatAsyncUploadTest extends TapAsyncUploadTest {

    private static final Logger log = Logger.getLogger(CatAsyncUploadTest.class);

    // prod CADC minoc
    static final URI STORAGE_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/cadc/minoc");

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
    }

    public CatAsyncUploadTest() {
        super(Constants.RESOURCE_ID);
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", CatSyncUploadTest.class);
        setTestFile(f);
        setTestURL(getVOTableURL(f));
    }

    static URL getVOTableURL(File f) {
        try {
            String tableName = "tab_" + System.currentTimeMillis();

            File cert = FileUtil.getFileFromResource("tmpops.pem", CatAsyncUploadTest.class);
            Subject subject = SSLUtil.createSubject(cert);

            RegistryClient reg = new RegistryClient();
            URL storageURL = reg.getServiceURL(STORAGE_RESOURCE_ID, Standards.SI_FILES, AuthMethod.CERT);
            URL putURL = new URL(storageURL.toExternalForm() + "/tmp:youcat-int-test/" + tableName + ".xml");
            HttpUpload httpUpload = new HttpUpload(f, putURL);
            httpUpload.setRequestProperty(HttpTransfer.CONTENT_TYPE, "text/xml");
            Subject.doAs(subject, new RunnableAction(httpUpload));

            // Error during the upload, throw an exception.
            if (httpUpload.getThrowable() != null) {
                throw new RuntimeException("setup failed to store VOTable upload ", httpUpload.getThrowable());
            }

            return putURL;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to generate put URL", ex);
        }
    }
    
    @Test
    public void testMultipleInlineUpload() {
        try {
            final String t1 = "table1";
            final String t2 = "table2";
            final File f1 = FileUtil.getFileFromResource("TAPUploadTest-1.xml", CatSyncUploadTest.class);
            final File f2 = FileUtil.getFileFromResource("TAPUploadTest-2.xml", CatSyncUploadTest.class);
            
            List<String> ups = new ArrayList<>();
            ups.add(t1 + ",param:upload1");
            ups.add(t2 + ",param:upload2");
            
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("upload1", f1);
            params.put("upload2", f2);
            params.put("UPLOAD", ups);
            params.put("LANG", "ADQL");
            params.put("QUERY", "select * from tap_upload." + t1 + " join tap_upload." + t2 + " on t1.str1=t2.Collection");
            
            URL jobURL = createAsyncParamJob("testMultipleInlineUpload", params);
            log.info("created: " + jobURL);
            
            // read the job and check that all UPLOAD param values were re-written
            // param:upload1 -> https://..../something
            // param:upload2 -> https://..../something-else
            HttpGet get = new HttpGet(jobURL, true);
            get.prepare();
            JobReader jr = new JobReader();
            Job job = jr.read(get.getInputStream());
            Assert.assertNotNull(job);
            for (Parameter p : job.getParameterList()) {
                if ("UPLOAD".equals(p.getName())) {
                    log.info("upload: " + p.getValue());
                    int i = p.getValue().indexOf(',');
                    String tname = p.getValue().substring(0, i);
                    String tloc = p.getValue().substring(i + 1);
                    log.info("tname=" + tname + " tloc=" + tloc);
                    if (t1.equals(tname)) {
                        Assert.assertTrue(tloc.contains("upload1"));
                    } else if (t2.equals(tname)) {
                        Assert.assertTrue(tloc.contains("upload2"));
                    } else {
                        Assert.fail("upload param missing table name: " + p.getValue());
                    }
                    Assert.assertTrue(tloc.startsWith("https://"));
                }
            }

            // only testing that multiple upload tables get handled as expected
            // no need to run query

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
