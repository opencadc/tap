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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableGroup;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.datalink.ServiceDescriptorTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

public class TemplateDAOTest {
    private static final Logger log = Logger.getLogger(TemplateDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
    }

    static final String OWNER_CERT = "youcat-owner.pem";    // own test schema
    static final String MEMBER_CERT = "youcat-member.pem"; // member of group

    final Subject owner;
    final Subject member;
    final DataSource dataSource;

    @Before
    public void setUp() throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        String sql = "DELETE FROM tap_schema.ServiceDescriptorTemplate";
        jdbc.update(sql);
        log.debug(sql);
    }

    public TemplateDAOTest() {
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("TAP_SCHEMA_TEST", "cadctest");
            dataSource = DBUtil.getDataSource(cc, true, true);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());

            File cert = FileUtil.getFileFromResource(OWNER_CERT, AbstractTablesTest.class);
            owner = SSLUtil.createSubject(cert);
            log.debug("created owner: " + owner);

            cert = FileUtil.getFileFromResource(MEMBER_CERT, AbstractTablesTest.class);
            member = SSLUtil.createSubject(cert);
            log.debug("created member: " + member);

        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
    }

    @Test
    public void testTemplateDAO() {
        try {
            String testName = "test-template";
            String testTemplate = getTestTemplate("caomPublisherID");

            ServiceDescriptorTemplate expected = new ServiceDescriptorTemplate(testName, testTemplate);
            expected.owner = owner;
            expected.ownerID = AuthenticationUtil.getIdentityManager().toOwner(owner);
            log.debug("expected: " + expected);

            // PUT a descriptor
            TemplateDAO templateDAO = new TemplateDAO(dataSource);
            templateDAO.put(expected);

            // GET the descriptor
            ServiceDescriptorTemplate actual = templateDAO.get(owner, testName);
            validate(expected, actual, true);

            // UPDATE the descriptor
            expected.owner = member;
            expected.ownerID = AuthenticationUtil.getIdentityManager().toOwner(member);
            templateDAO.put(expected);

            // GET the descriptor again to verify update
            actual = templateDAO.get(member, testName);
            Assert.assertNotNull("expected null", actual);
            validate(expected, actual, true);

            // DELETE the descriptor
            templateDAO.delete(member, testName);

            // GET the descriptor again to verify deletion
            actual = templateDAO.get(member, testName);
            Assert.assertNull("expected null", actual);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void validate(ServiceDescriptorTemplate expected, ServiceDescriptorTemplate actual, boolean checkOwner) {
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getTemplate(), actual.getTemplate());
        if (checkOwner) {
            Assert.assertEquals(expected.owner, actual.owner);
            Assert.assertEquals(expected.ownerID, actual.ownerID);
        }
        Assert.assertEquals(expected.getIdentifiers(), actual.getIdentifiers());
        Assert.assertNotNull(actual.getResource());
        VOTableResource expectedResource = expected.getResource();
        VOTableResource actualResource = actual.getResource();
        Assert.assertEquals(expectedResource.getType(), actualResource.getType());
        Assert.assertEquals(expectedResource.utype, actualResource.utype);
        Assert.assertFalse(actual.getResource().getGroups().isEmpty());
        VOTableGroup expectedGroup = expectedResource.getGroups().get(0);
        VOTableGroup actualGroup = actualResource.getGroups().get(0);
        Assert.assertEquals(expectedGroup.getName(), actualGroup.getName());
        Assert.assertEquals(expectedGroup.getParams().size(), actualGroup.getParams().size());
        Assert.assertEquals(expectedGroup.getParams().get(0).getName(), actualGroup.getParams().get(0).getName());
    }

    private String getTestTemplate(String identifier) throws IOException {
        File testFile = FileUtil.getFileFromResource("valid-template.xml", TemplateDAOTest.class);
        String template = Files.readString(testFile.toPath());
        return template.replace("IDENTIFIER", identifier);
    }

}
