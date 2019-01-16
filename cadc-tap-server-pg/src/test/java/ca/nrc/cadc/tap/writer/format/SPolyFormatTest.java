/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package ca.nrc.cadc.tap.writer.format;

import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import static org.junit.Assert.assertEquals;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import ca.nrc.cadc.util.Log4jInit;
import java.util.List;

/**
 *
 * @author jburke
 */
public class SPolyFormatTest
{
    private static final Logger log = Logger.getLogger(SPointFormatTest.class);
    static
    {
        Log4jInit.setLevel("ca", Level.INFO);
    }
    private static final String SPOLYGON = " {(0.0349065850398866, 0.0349065850398866),(0.0349065850398866, 0.0698131700797732),(0.0523598775598299, 0.0523598775598299)}";
    private static final String DALI_POLYGON = "2.0 2.0 2.0 4.0 3.0 3.0";
    
    public SPolyFormatTest() { }

    /**
     * Test of format method, of class SPolyFormatter.
     */
    @Test
    public void testFormat()
    {
        log.debug("testFormat");
        Polygon poly = new Polygon();
        poly.getVertices().add(new Point(2.0, 2.0));
        poly.getVertices().add(new Point(2.0, 4.0));
        poly.getVertices().add(new Point(3.0, 3.0));
        SPolyFormat fmt = new SPolyFormat();
        String expResult = DALI_POLYGON;
        String result = fmt.format(poly);
        assertEquals(expResult.toUpperCase(), result.toUpperCase());
        log.info("testFormat passed");
    }

    /**
     * Test of getPolygon method, of class SPolyFormatter.
     */
    @Test
    public void testGetPosition()
    {
        log.debug("testGetPolygon");

        SPolyFormat fmt = new SPolyFormat();
        Polygon polygon = fmt.getPolygon(SPOLYGON);
        List<Point> coordPairs = polygon.getVertices();
        assertEquals("", 2.0, coordPairs.get(0).getLongitude(), 0.01);
        assertEquals("", 2.0, coordPairs.get(0).getLatitude(), 0.01);
        assertEquals("", 2.0, coordPairs.get(1).getLongitude(), 0.01);
        assertEquals("", 4.0, coordPairs.get(1).getLatitude(), 0.01);
        assertEquals("", 3.0, coordPairs.get(2).getLongitude(), 0.01);
        assertEquals("", 3.0, coordPairs.get(2).getLatitude(), 0.01);
    }

}