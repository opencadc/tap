package org.opencadc.youcat;

import ca.nrc.cadc.tap.integration.TapSyncUploadTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
// NOTE: disabled until we support UPLOAD in cadc-rest based UWS async endpoint
public class CatSyncUploadTest extends TapSyncUploadTest {
    private static final Logger log = Logger.getLogger(CatSyncUploadTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.cat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
    }

    public CatSyncUploadTest() {
        super(Constants.RESOURCE_ID);
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", CatSyncUploadTest.class);
        setTestFile(f);
        setTestURL(CatAsyncUploadTest.getVOTableURL(f));
    }
}
