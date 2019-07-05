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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.vosi.actions;

import ca.nrc.cadc.rest.InlineContentHandler.Content;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.actions.PostPermissionsAction.PermissionsInlineContentHandler;

import java.io.ByteArrayInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the inline content handler for permissions.
 *
 * @author majorb
 */
public class PostPermissionsActionTest {
    
    private static final Logger log = Logger.getLogger(PostPermissionsActionTest.class);
    
    static {
        Log4jInit.setLevel("ca.nrc.cadc.vosi", Level.INFO);
    }

    public PostPermissionsActionTest() {
    }
    
    @Test
    public final void testWrongContentType() {
        log.debug("testWrongContentType");
        try {
            String content = TablesAction.PUBLIC_KEY + "=" + "true";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, "wrong/type", in);

            Assert.fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public final void testSetOwner() {
        log.debug("testSetOwner");
        try {
            String content = TablesAction.OWNER_KEY + "=" + "someone";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);

            Assert.fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testClearOwner() {
        log.debug("testClearOwner");
        try {
            String content = TablesAction.OWNER_KEY + "=";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            // attempts to clear are tolerated
            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            
            Assert.assertNull(c);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testSetPublicTrue() {
        log.debug("testSetPublicTrue");
        try {
            String content = TablesAction.PUBLIC_KEY + "=" + "true";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertTrue(tp.isPublic());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testSetPublicFalse() {
        log.debug("testSetPublicFalse");
        try {
            String content = TablesAction.PUBLIC_KEY + "=" + "false";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertFalse(tp.isPublic());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testSetPublicBadValue() {
        log.debug("testSetPublicBadValue");
        try {
            String content = TablesAction.PUBLIC_KEY + "=" + "yes";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            
            Assert.fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testSetReadGroup() {
        log.debug("testSetReadGroup");
        try {
            String testGroupURI = "ivo://cadc.nrc.ca/gms?group";
            String content = TablesAction.RGROUP_KEY + "=" + testGroupURI;
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertEquals(testGroupURI, tp.getReadGroup().toString());
            Assert.assertFalse(tp.isClearReadGroup());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testSetReadWriteGroup() {
        log.debug("testSetReadWriteGroup");
        try {
            String testGroupURI = "ivo://cadc.nrc.ca/gms?group";
            String content = TablesAction.RWGROUP_KEY + "=" + testGroupURI;
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertEquals(testGroupURI, tp.getReadWriteGroup().toString());
            Assert.assertFalse(tp.isClearReadWriteGroup());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testClearReadGroup() {
        log.debug("testSetReadGroup");
        try {
            String content = TablesAction.RGROUP_KEY + "=";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertNull(tp.getReadGroup());
            Assert.assertTrue(tp.isClearReadGroup());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testClearReadWriteGroup() {
        log.debug("testSetReadGroup");
        try {
            String content = TablesAction.RWGROUP_KEY + "=";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertNull(tp.getReadWriteGroup());
            Assert.assertTrue(tp.isClearReadWriteGroup());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testBadReadGroup() {
        log.debug("testBadReadGroup");
        try {
            String content = TablesAction.RGROUP_KEY + "=" + "malformedURI";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            
            Assert.fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testBadReadWriteGroup() {
        log.debug("testBadReadWriteGroup");
        try {
            String content = TablesAction.RWGROUP_KEY + "=" + "malformedURI";
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            
            Assert.fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            log.info("expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testMultipleProperties() {
        log.debug("testMultipleProperties");
        try {
            String testGroupURI1 = "ivo://cadc.nrc.ca/gms?group1";
            String testGroupURI2 = "ivo://cadc.nrc.ca/gms?group2";
            String content = TablesAction.PUBLIC_KEY + "=true\n" +
                             TablesAction.RGROUP_KEY + "=" + testGroupURI1 + "\n" +
                             TablesAction.RWGROUP_KEY + "=" + testGroupURI2;
            ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());

            PermissionsInlineContentHandler p = new PostPermissionsAction().new PermissionsInlineContentHandler();
            Content c = p.accept(PostPermissionsAction.TAP_PERMISSIONS_CONTENT, TablesAction.PERMS_CONTENTTYPE, in);
            TapPermissions tp = (TapPermissions) c.value;
            Assert.assertTrue(tp.isPublic());
            Assert.assertEquals(testGroupURI1, tp.getReadGroup().toString());
            Assert.assertEquals(testGroupURI2, tp.getReadWriteGroup().toString());
            Assert.assertFalse(tp.isClearReadGroup());
            Assert.assertFalse(tp.isClearReadWriteGroup());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
