/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.vosi.actions.TablesInputHandler;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class GetActionTest extends AbstractTablesTest {

    @Test
    public void testValidateTable() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);

            String testTable = testSchemaName + ".testPutWarnings";

            // cleanup just in case
            doDelete(schemaOwner, testTable, true);

            VOTableTable vtab = new VOTableTable();
            vtab.getFields().add(new VOTableField("c0", TapDataType.STRING.getDatatype(), TapDataType.STRING.arraysize));
            vtab.getFields().add(new VOTableField("c1", TapDataType.INTEGER.getDatatype()));

            VOTableField field1 = new VOTableField("Field1", TapDataType.DOUBLE.getDatatype());
            field1.ucd = "arith.factor";
            field1.unit = "K/S";
            vtab.getFields().add(field1);

            VOTableField field2 = new VOTableField("Field2", TapDataType.DOUBLE.getDatatype());
            field2.ucd = "obs.atmos.turbulence.isoplanatic";
            field2.unit = "1.898E27kg";
            vtab.getFields().add(field2);

            //-----
            VOTableResource vres = new VOTableResource("results");
            vres.setTable(vtab);
            final VOTableDocument doc = new VOTableDocument();
            doc.getResources().add(vres);

            // create
            URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + testTable);
            OutputStreamWrapper src = new OutputStreamWrapper() {
                @Override
                public void write(OutputStream out) throws IOException {
                    VOTableWriter w = new VOTableWriter(VOTableWriter.SerializationType.TABLEDATA);
                    w.write(doc, out);
                }
            };
            HttpUpload put = new HttpUpload(src, tableURL);
            put.setRequestProperty("Content-Type", TablesInputHandler.VOTABLE_TYPE);
            Subject.doAs(schemaOwner, new RunnableAction(put));

            Assert.assertNull("throwable", put.getThrowable());
            Assert.assertEquals("response code", 200, put.getResponseCode());

            // Validate the table
            URL getURL = new URL(certTablesURL.toExternalForm() + "/" + testTable + "?action=validate");
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            HttpGet check = new HttpGet(getURL, stream);
            Subject.doAs(schemaOwner, new RunnableAction(check));

            Assert.assertNull("throwable", check.getThrowable());
            Assert.assertEquals("response code", 200, check.getResponseCode());
            String validationContent = stream.toString(StandardCharsets.UTF_8);
            Assert.assertEquals("OK", validationContent);
        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
