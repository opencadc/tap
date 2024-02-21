/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.tap;

import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.LongFormat;
import ca.nrc.cadc.dali.util.StringFormat;
import ca.nrc.cadc.dali.util.URIFormat;
import ca.nrc.cadc.dali.util.UTCTimestampFormat;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.io.InconsistentTableDataException;

/**
 *
 * @author pdowler
 */
public class TsvIteratorTest {
    private static final Logger log = Logger.getLogger(TsvIteratorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.tap", Level.INFO);
    }
    
    public TsvIteratorTest() { 
    }
    
    @Test
    public void testEmptyStream() throws Exception {
        List<Format> formatters = new ArrayList<>();
        formatters.add(new URIFormat());
        
        byte[] empty = new byte[0];
        ByteArrayInputStream bis = new ByteArrayInputStream(empty);

        try {
            TsvIterator<URI> iter = new TsvIterator<>(new FourColMapper(), formatters, "text/tab-separated-values", bis);
            Assert.fail("expected IOException, got an iterator");
        } catch (IOException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testNoRows() throws Exception {
        List<Format> formatters = new ArrayList<>();
        formatters.add(new URIFormat());
        formatters.add(new StringFormat());
        formatters.add(new LongFormat());
        formatters.add(new UTCTimestampFormat());
        
        String msg = "a\tb\tc\td\n";
        byte[] bytes = msg.getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

        try {
            TsvIterator<URI> iter = new TsvIterator<>(new FourColMapper(), formatters, "text/tab-separated-values", bis);
            Assert.assertFalse(iter.hasNext());
        } catch (IOException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testErrorStream() throws Exception {
        List<Format> formatters = new ArrayList<>();
        formatters.add(new URIFormat());
        formatters.add(new StringFormat());
        formatters.add(new LongFormat());
        formatters.add(new UTCTimestampFormat());
        
        String msg = "internal\tserver error"; // also looks like 2 columns
        byte[] bytes = msg.getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

        try {
            TsvIterator<URI> iter = new TsvIterator<>(new FourColMapper(), formatters, "text/tab-separated-values", bis);
            Assert.fail("expected IOException, got an iterator with hasNext = " + iter.hasNext());
        } catch (InconsistentTableDataException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testErrorMidStream() throws Exception {
        List<Format> formatters = new ArrayList<>();
        formatters.add(new URIFormat());
        formatters.add(new StringFormat());
        formatters.add(new LongFormat());
        formatters.add(new UTCTimestampFormat());
        
        String msg = "a\tb\tc\td\n" 
                + "foo:bar1\tabc\t123\t2000-01-01T01:02:03\n"
                + "foo:bar2\toops\n"
                + "foo:bar3\tabc\t123\t2000-01-01T01:02:03\n"
                ;
        byte[] bytes = msg.getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

        try {
            TsvIterator<URI> iter = new TsvIterator<>(new FourColMapper(), formatters, "text/tab-separated-values", bis);
            Assert.assertTrue(iter.hasNext());
            URI v1 = iter.next();
            log.info("found: " + v1);
            Assert.assertTrue(iter.hasNext());
            URI v2 = iter.next();
            
            Assert.fail("expected RowMapException, got " + v2);
        } catch (RowMapException expected) {
            log.info("caught expected: " + expected + " cause: " + expected.getCause());
            
        }
    }
    
    @Test
    public void testWrongContentType() throws Exception {
        List<Format> formatters = new ArrayList<>();
        formatters.add(new URIFormat());
        
        byte[] empty = new byte[0];
        ByteArrayInputStream bis = new ByteArrayInputStream(empty);

        try {
            TsvIterator<URI> iter = new TsvIterator<>(new FourColMapper(), formatters, "text/plain", bis);
            Assert.fail("expected UnsupportedOperationException, got an iterator");
        } catch (UnsupportedOperationException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testNullContentType() throws Exception {
        List<Format> formatters = new ArrayList<>();
        formatters.add(new URIFormat());
        
        byte[] empty = new byte[0];
        ByteArrayInputStream bis = new ByteArrayInputStream(empty);

        try {
            TsvIterator<URI> iter = new TsvIterator<>(new FourColMapper(), formatters, null, bis);
            Assert.fail("expected UnsupportedOperationException, got an iterator");
        } catch (UnsupportedOperationException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    private class FourColMapper implements TapRowMapper<URI> {

        @Override
        public URI mapRow(List<Object> row) {
            URI ret = (URI) row.get(0);
            String c2 = (String) row.get(1);
            Long c3 = (Long) row.get(2);
            Date c4 = (Date) row.get(3);
            return ret;
        }
        
    }
}
