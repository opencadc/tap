/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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
 ************************************************************************
 */

package org.opencadc.youcat;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.tap.schema.TapPermissions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

public class ParquetWriterTest extends AbstractTablesTest {
    private static final Logger log = Logger.getLogger(ParquetWriterTest.class);
    private static URL url;

    @Test
    public void testWriteParquetWithExistingTable() throws Exception {
        String adql = "SELECT * from tap_schema.columns";

        Map<String, Object> params = new TreeMap<>();
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
            extractVOTableFromOutputStream(out, adql);
        } catch (Exception e) {
            log.error("unexpected exception", e);
            Assert.fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testWriteParquetWithCustomTable() throws Exception {

        String testTable = testSchemaName + ".testWriteParquet";

        createCustomTable(testTable);

        addDataToCustomTable(testTable);

        // get votable from votable query
        VOTableTable voTableTable = doQueryForVOT(testTable);

        // get votable from parquet file
        VOTableTable voTableFromParquet = getVOTableFromParquet(testTable);

        compareVOTables(voTableTable, voTableFromParquet);

        doDelete(schemaOwner, testTable, false);
    }

    private static VOTableTable extractVOTableFromOutputStream(ByteArrayOutputStream out, String adql) throws IOException {
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        VOTableDocument voTableDocument = reader.read(inputStream);
        Assert.assertNotNull(voTableDocument.getResources());

        VOTableResource results = voTableDocument.getResourceByType("results");
        Assert.assertNotNull(results);
        log.info(results.getTable().getFields().size() + " columns found.");
        Assert.assertFalse(results.getTable().getFields().isEmpty());

        boolean queryFound = false;
        boolean queryStatusFound = false;

        for (VOTableInfo voTableInfo : results.getInfos()) {
            if (voTableInfo.getName().equals("QUERY")) {
                queryFound = true;
                Assert.assertEquals(adql, voTableInfo.getValue());
            } else if (voTableInfo.getName().equals("QUERY_STATUS")) {
                queryStatusFound = true;
                Assert.assertEquals("OK", voTableInfo.getValue());
            }
        }

        Assert.assertTrue(queryFound);
        Assert.assertTrue(queryStatusFound);

        Assert.assertNotNull(results.getTable());
        return results.getTable();
    }

    private void createCustomTable(String testTable) throws Exception {
        clearSchemaPerms();

        TapPermissions tapPermissions = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tapPermissions, 200);

        doCreateTable(schemaOwner, testTable);
        setPerms(schemaOwner, testTable, tapPermissions, 200);
    }

    private void addDataToCustomTable(String testTable) throws MalformedURLException, PrivilegedActionException, URISyntaxException {
        String data = doPrepareDataAllDataTypesTSV();

        doUploadTSVData(testTable, data);
    }

    private void compareVOTables(VOTableTable voTableFromVOTableWriter, VOTableTable voTableFromParquet) throws IOException, URISyntaxException {
        Assert.assertEquals(voTableFromVOTableWriter.getFields().size(), voTableFromParquet.getFields().size());
        Assert.assertEquals(voTableFromVOTableWriter.getInfos().size(), voTableFromParquet.getInfos().size());

        // compare the fields
        for (int i = 0; i < voTableFromVOTableWriter.getFields().size(); i++) {
            VOTableField field1 = voTableFromVOTableWriter.getFields().get(i);
            VOTableField field2 = voTableFromParquet.getFields().get(i);

            Assert.assertEquals(field1.getName(), field2.getName());

            if (field1.getDatatype().equals("short")) {
                Assert.assertEquals("int", field2.getDatatype());
            } else {
                Assert.assertEquals(field1.getDatatype(), field2.getDatatype());
                Assert.assertEquals(field1.getArraysize(), field2.getArraysize());
            }
        }

        verifyAllDataTypes(voTableFromParquet,true, false);
    }

    private VOTableTable getVOTableFromParquet(String testTable) throws Exception {
        String adql = "SELECT * from " + testTable;

        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "parquet");

        TapClient tapClient = new TapClient(Constants.RESOURCE_ID);
        url = tapClient.getSyncURL(Standards.SECURITY_METHOD_ANON);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String result = Subject.doAs(schemaOwner, new AuthQueryTest.SyncQueryAction(anonQueryURL, params, out, "application/vnd.apache.parquet"));
        Assert.assertNotNull(result);

        return extractVOTableFromOutputStream(out, adql);
    }

}
