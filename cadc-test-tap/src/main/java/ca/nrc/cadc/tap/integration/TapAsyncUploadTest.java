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

package ca.nrc.cadc.tap.integration;

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

    private static final long TIMEOUT = 60 * 1000L;
    
    private File testFile;
    private URL testURL;
    private final StringIDGenerator idgen = new RandomStringGenerator(8);

    public TapAsyncUploadTest(URI resourceID) {
        super(resourceID, Standards.TAP_10, Standards.INTERFACE_PARAM_HTTP, "async");
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
