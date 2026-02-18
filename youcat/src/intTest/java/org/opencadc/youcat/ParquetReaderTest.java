/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2024.
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
import ca.nrc.cadc.dali.tables.parquet.ParquetWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.io.RandomAccessFile;
import ca.nrc.cadc.io.RandomAccessSource;
import ca.nrc.cadc.net.RandomAccessURL;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.tap.schema.TapPermissions;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class ParquetReaderTest extends AbstractTablesTest {

    private static final Logger log = Logger.getLogger(ParquetReaderTest.class);

    public ParquetReaderTest() {
        super();
    }

    @Test
    public void testParquetReader() throws Exception {
        log.info("Running ParquetReaderTest");
        testParquetReader(true);
        testParquetReader(false);
        testRemoteAccessURL(true);
        testRemoteAccessURL(false);
    }

    private void testParquetReader(boolean withMetadata) throws Exception {
        String testTable = testSchemaName + ".testReadParquet";

        // create table
        clearSchemaPerms();

        TapPermissions tapPermissions = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tapPermissions, 200);

        doCreateTable(schemaOwner, testTable);
        setPerms(schemaOwner, testTable, tapPermissions, 200);

        // load table data
        loadTableData(testTable);

        // Write out the table into Parquet format
        ByteArrayOutputStream out = getParquetStreamingData(withMetadata, testTable);

        // Read the Parquet data into a VOTableDocument
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        try (VOTableDocument voDocFromParquetReader = reader.read(inputStream)) {
            Assert.assertNotNull(voDocFromParquetReader);
            verifyAllDataTypes(voDocFromParquetReader.getResourceByType("results").getTable(), true, !withMetadata);
        }

        // Test local parquet file via FileRandomAccessSource
        File tempFile = File.createTempFile("parquetTest", ".parquet");
        tempFile.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(out.toByteArray());
        }

        RandomAccessSource randomAccessSource = new RandomAccessFile(tempFile, "r");
        try (VOTableDocument voDocFromParquetReader = reader.read(randomAccessSource)) {
            Assert.assertNotNull(voDocFromParquetReader);
            verifyAllDataTypes(voDocFromParquetReader.getResourceByType("results").getTable(), true, !withMetadata);
        }
    }

    private void testRemoteAccessURL(boolean withMetadata) throws IOException, ResourceNotFoundException, URISyntaxException {
        ParquetReader reader = new ParquetReader();
        String remoteFileName = withMetadata ? "parquet-with-metadata" : "parquet-without-metadata";
        URL artifactURL = new URL("https://ws-cadc.canfar.net/vault/files/CADC/test-data/tap-upload/" + remoteFileName + ".parquet");

        RandomAccessSource randomAccessSource = new RandomAccessURL(artifactURL);
        try (VOTableDocument voDocFromParquetReader = reader.read(randomAccessSource)) {
            Assert.assertNotNull(voDocFromParquetReader);
            verifyAllDataTypes(voDocFromParquetReader.getResourceByType("results").getTable(), true, !withMetadata);
        }
    }

    // Write out the table into Parquet format - Use writer directly to test it with and without metadata
    private ByteArrayOutputStream getParquetStreamingData(boolean withMetadata, String testTable) throws Exception {
        // get data in votable format
        String result = doQuery(testTable);
        Assert.assertNotNull(result);

        VOTableReader voTableReader = new VOTableReader();
        VOTableDocument voTableDocument = voTableReader.read(result);

        // write the votable to parquet format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetWriter parquetWriter = new ParquetWriter(withMetadata);
        parquetWriter.write(voTableDocument, out);
        return out;
    }

    private void loadTableData(String testTable) throws Exception {
        String data = doPrepareDataAllDataTypesTSV();

        doUploadTSVData(testTable, data);
    }

}