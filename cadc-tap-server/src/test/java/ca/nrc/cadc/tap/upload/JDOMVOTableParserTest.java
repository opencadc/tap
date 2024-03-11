/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.upload;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.io.ByteLimitExceededException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;

public class JDOMVOTableParserTest {
    private static final Logger LOGGER = LogManager.getLogger(JDOMVOTableParserTest.class);

    static {
        LOGGER.setLevel(Level.DEBUG);
    }

    @Test
    public void testUploadValidateUnlimited() throws Exception {
        final JDOMVOTableParser testSubject = new JDOMVOTableParser();

        final File uploadFile = createUploadFile("testuploadunlimited", "results",
                                                  4, 200);

        final UploadTable uploadTable = new UploadTable("test1", "jobid1", uploadFile.toURI());

        testSubject.setUpload(uploadTable, null);
        testSubject.verifyUploadTable();
    }

    @Test
    public void testUploadValidateRowLimit() throws Exception {
        final JDOMVOTableParser testSubject = new JDOMVOTableParser();

        final File uploadFile = createUploadFile("testuploadrowlimit", "results",
                2, 5);

        final UploadTable uploadTable = new UploadTable("test2", "jobid2", uploadFile.toURI());

        testSubject.setUpload(uploadTable, new UploadLimits(5L * 1024L * 1024L, 2, null));

        try {
            testSubject.verifyUploadTable();
            Assert.fail("Should throw IOException here.");
        } catch (IOException ioException) {
            Assert.assertEquals("Wrong message.", "Row count exceeds maximum of 2",
                    ioException.getMessage());
        }
    }

    @Test
    public void testUploadValidateColumnLimit() throws Exception {
        final JDOMVOTableParser testSubject = new JDOMVOTableParser();

        final File uploadFile = createUploadFile("testuploadcolumnlimit", "results",
                12, 500);

        final UploadTable uploadTable = new UploadTable("test3", "jobid3", uploadFile.toURI());

        testSubject.setUpload(uploadTable, new UploadLimits(5L * 1024L * 1024L, null, 4));

        try {
            testSubject.verifyUploadTable();
            Assert.fail("Should throw IOException here.");
        } catch (IOException ioException) {
            Assert.assertEquals("Wrong message.", "Column count exceeds maximum of 4",
                    ioException.getMessage());
        }
    }

    @Test
    public void testUploadValidateByteLimit() throws Exception {
        final JDOMVOTableParser testSubject = new JDOMVOTableParser();

        final File uploadFile = createUploadFile("testuploadbytelimit", "results",
                12, 5000);

        final UploadTable uploadTable = new UploadTable("test4", "jobid4", uploadFile.toURI());

        testSubject.setUpload(uploadTable, new UploadLimits(1024L, null, null));

        try {
            testSubject.verifyUploadTable();
            Assert.fail("Should throw ByteLimitExceededException here.");
        } catch (ByteLimitExceededException ioException) {
            Assert.assertEquals("Wrong message.", 1024L, ioException.getLimit(), 0L);
        }
    }

    @Test
    public void testUploadValidateCombinationLimit1() throws Exception {
        final JDOMVOTableParser testSubject = new JDOMVOTableParser();

        final File uploadFile = createUploadFile("testuploadcombinationlimit2", "results",
                12, 5000);

        final UploadTable uploadTable = new UploadTable("test5", "jobid5", uploadFile.toURI());

        testSubject.setUpload(uploadTable, new UploadLimits(1024L * 1024L, 2000, null));

        try {
            testSubject.verifyUploadTable();
            Assert.fail("Should throw IOException here.");
        } catch (ByteLimitExceededException ioException) {
            Assert.assertEquals("Wrong message.", 1024L * 1024L, ioException.getLimit(), 0L);
        }
    }

    @Test
    public void testUploadValidateCombinationLimit2() throws Exception {
        final JDOMVOTableParser testSubject = new JDOMVOTableParser();

        final File uploadFile = createUploadFile("testuploadcombinationlimit2", "results",
                12, 5000);

        final UploadTable uploadTable = new UploadTable("test6", "jobid6", uploadFile.toURI());

        testSubject.setUpload(uploadTable, new UploadLimits(5 * 1024L * 1024L, 2000, null));

        try {
            testSubject.verifyUploadTable();
            Assert.fail("Should throw IOException here.");
        } catch (IOException ioException) {
            Assert.assertEquals("Wrong message.", "Row count exceeds maximum of 2000",
                    ioException.getMessage());
        }
    }

    private File createUploadFile(final String namePrefix, final String resourceType, final int columnCount,
                                  final int rowCount) throws Exception {
        final File file = Files.createTempFile(namePrefix, ".xml").toFile();
        LOGGER.debug("Created " + file.getAbsolutePath());
        try (final Writer uploadWriter = new FileWriter(file)) {
            final VOTableWriter voTableWriter = new VOTableWriter("text/xml");
            final VOTableDocument voTableDocument = VOTableDocumentGenerator.generateSingleResourceTable(resourceType,
                                                                                                         columnCount,
                                                                                                         rowCount);
            voTableWriter.write(voTableDocument, uploadWriter);
            uploadWriter.flush();
            LOGGER.debug("Finished writing " + file.getAbsolutePath());
        }

        return file;
    }
}
