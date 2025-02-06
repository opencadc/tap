/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 5 $
*
************************************************************************
 */

package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobListReader;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.JobRef;
import ca.nrc.cadc.uws.Result;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.StringReader;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

/**
 * Half-decent test that authenticated queries work.
 *
 * @author pdowler
 */
public class AuthQueryTest {

    private static final Logger log = Logger.getLogger(AuthQueryTest.class);

    static final Subject subjectWithGroups;
    static final Principal userWithGroups;
    static URL syncCertURL;
    static URL asyncCertURL;

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);

        // need to read cert so we have creds to make the fake GMS call
        File cf = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", AuthQueryTest.class);
        subjectWithGroups = SSLUtil.createSubject(cf);
        userWithGroups = subjectWithGroups.getPrincipals(X500Principal.class).iterator().next();
        log.debug("created subjectWithGroups: " + subjectWithGroups);

        try {
            TapClient tc = new TapClient(Constants.RESOURCE_ID);
            syncCertURL = tc.getSyncURL(Standards.SECURITY_METHOD_CERT);
            asyncCertURL = tc.getAsyncURL(Standards.SECURITY_METHOD_CERT);
        } catch (Exception ex) {
            log.error("TEST SETUP BUG: failed to find TAP URL", ex);
        }
    }

    static class SyncQueryAction implements PrivilegedExceptionAction<String> {

        private URL url;
        private Map<String, Object> params;
        ByteArrayOutputStream out;
        String contentType;

        public SyncQueryAction(URL url, Map<String, Object> params) {
            this.url = url;
            this.params = params;
            this.out = new ByteArrayOutputStream();
            this.contentType = "application/x-votable+xml";
        }
        public SyncQueryAction(URL url, Map<String, Object> params, ByteArrayOutputStream out, String contentType) {
            this.url = url;
            this.params = params;
            this.out = out;
            this.contentType = contentType;
        }

        @Override
        public String run()
                throws Exception {
            HttpPost doit = new HttpPost(url, params, out);
            doit.run();

            if (doit.getThrowable() != null) {
                log.error("Post failed", doit.getThrowable());
                Assert.fail("exception on post: " + doit.getThrowable());
            }

            int code = doit.getResponseCode();
            Assert.assertEquals(200, code);

            String contentType = doit.getContentType();
            String result = out.toString();

            log.debug("contentType: " + contentType);
            log.debug("respnse:\n" + result);

            Assert.assertEquals(this.contentType, contentType);

            return result;
        }

    }

    static class AsyncQueryAction implements PrivilegedExceptionAction<Job> {

        private URL url;
        private Map<String, Object> params;

        public AsyncQueryAction(URL url, Map<String, Object> params) {
            this.url = url;
            this.params = params;
        }

        @Override
        public Job run()
                throws Exception {
            HttpPost doit = new HttpPost(url, params, false);
            doit.run();

            if (doit.getThrowable() != null) {
                log.error("Post failed", doit.getThrowable());
                Assert.fail("exception on post: " + doit.getThrowable());
            }

            int code = doit.getResponseCode();
            Assert.assertEquals(303, code);

            URL jobURL = doit.getRedirectURL();

            // exec the job
            URL phaseURL = new URL(jobURL.toString() + "/phase");
            Map<String, Object> nextParams = new HashMap<String, Object>();
            nextParams.put("PHASE", "RUN");
            doit = new HttpPost(phaseURL, nextParams, false);
            doit.run();

            if (doit.getThrowable() != null) {
                log.error("Post failed", doit.getThrowable());
                Assert.fail("exception on post: " + doit.getThrowable());
            }

            JobReader jr = new JobReader();
            Job job = null;
            URL waitURL = new URL(jobURL.toExternalForm() + "?WAIT=30");
            boolean done = false;
            while (!done) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                HttpDownload w = new HttpDownload(waitURL, out);
                w.run();
                job = jr.read(new ByteArrayInputStream(out.toByteArray()));
                ExecutionPhase ep = job.getExecutionPhase();
                done = !ep.isActive();
            }
            Assert.assertNotNull("job", job);

            return job;
        }

    }

    @Test
    public void testAnonQueryPrivateTable() {
        try {
            // select a row from a proprietary table in a publicly readable schema
            String adql = "SELECT TOP 1 * from cfht.luausdss";

            Map<String, Object> params = new TreeMap<String, Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpPost doit = new HttpPost(syncCertURL, params, bos);
            doit.setFollowRedirects(true);
            doit.run();

            Assert.assertEquals(400, doit.getResponseCode());
            Assert.assertNotNull(doit.getThrowable());

            VOTableReader r = new VOTableReader();
            VOTableDocument doc = r.read(doit.getThrowable().getMessage());
            VOTableResource vr = doc.getResourceByType("results");
            VOTableInfo queryStatus = null;
            for (VOTableInfo vi : vr.getInfos()) {
                if ("QUERY_STATUS".equals(vi.getName())) {
                    queryStatus = vi;
                }
            }
            Assert.assertNotNull(queryStatus);
            Assert.assertEquals("ERROR", queryStatus.getValue());
            Assert.assertNotNull(queryStatus.content);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testVOSAuthQuery() {
        try {
            String adql = "SELECT top 1 * from tap_schema.tables";

            Map<String, Object> params = new TreeMap<String, Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);
            params.put("DEST", "vos://cadc.nrc.ca~vault/CADCAuthtest1/test/youcat-testVOSAuthQuery");

            Job job = Subject.doAs(subjectWithGroups, new AsyncQueryAction(asyncCertURL, params));
            log.info("jobID: " + job.getID() + " phase: " + job.getExecutionPhase() + " error: " + job.getErrorSummary());
            Assert.assertTrue("job completed", job.getExecutionPhase().equals(ExecutionPhase.COMPLETED));
            for (Result r : job.getResultsList()) {
                log.info(r.getName() + ": " + r.getURI());
                if ("result".equals(r.getName())) { // spec result name since TAP-1.0
                    Assert.assertTrue("result stored in vault", r.getURI().toASCIIString().contains("vault"));
                }
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testAuthJobList() {
        try {
            URL listURL = new URL(asyncCertURL.toExternalForm() + "?LAST=5");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(asyncCertURL, bos);
            Subject.doAs(subjectWithGroups, new RunnableAction(get));
            log.info("code: " + get.getResponseCode() + " error: " + get.getThrowable());
            Assert.assertEquals(200, get.getResponseCode());
            Assert.assertNull(get.getThrowable());
            String xml = bos.toString("UTF-8");
            log.debug("job list:\n" + xml);
            JobListReader r = new JobListReader();
            List<JobRef> jobs = r.read(new StringReader(xml));
            for (JobRef j : jobs) {
                log.info("job: " + j);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
