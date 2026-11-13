
/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.tap.upload.datatype;


import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.util.DoubleIntervalFormat;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import static org.junit.Assert.*;
import org.junit.Test;


public class OracleDataTypeTest {
    @Test
    public void getDataType() {
        final OracleDataType testSubject = new OracleDataType();

        final TapDataType tapDataType = TapDataType.CLOB;
        final ColumnDesc columnDesc = new ColumnDesc("mytable", "mycolumn", tapDataType);
        final String dataType = testSubject.getDataType(columnDesc);

        assertEquals("Wrong datatype.", "VARCHAR2(3072)", dataType);
    }

    @Test
    public void getSizeDataType() {
        final OracleDataType testSubject = new OracleDataType();
        final TapDataType tapDataType = new TapDataType("char", "256*", null);

        final ColumnDesc columnDesc = new ColumnDesc("mytable", "mycolumn", tapDataType);
        final String dataType = testSubject.getDataType(columnDesc);

        assertEquals("Wrong datatype.", "VARCHAR2(256)", dataType);
    }

    @Test
    public void getDoubleDataType() {
        final OracleDataType testSubject = new OracleDataType();
        final ColumnDesc columnDesc = new ColumnDesc("mytable", "mycolumn", TapDataType.DOUBLE);
        assertEquals("Wrong datatype.", "BINARY_DOUBLE", testSubject.getDataType(columnDesc));
    }

    @Test
    public void getIntervalDataType() {
        final OracleDataType testSubject = new OracleDataType();
        final ColumnDesc columnDesc = new ColumnDesc("mytable", "mycolumn", TapDataType.INTERVAL);
        assertEquals("Wrong datatype.", "VARCHAR2(128)", testSubject.getDataType(columnDesc));
    }

    @Test
    public void getDoubleArrayDataType() {
        final OracleDataType testSubject = new OracleDataType();
        final TapDataType tapDataType = new TapDataType("double", "8", null);
        final ColumnDesc columnDesc = new ColumnDesc("mytable", "mycolumn", tapDataType);
        assertEquals("Wrong datatype.", "VARCHAR2(3072)", testSubject.getDataType(columnDesc));
    }

    @Test
    public void getUploadVOTable13Xtypes() {
        final OracleDataType mapper = new OracleDataType();

        assertEquals("VARCHAR2(36)",
                     mapper.getDataType(new ColumnDesc("t", "planeID",
                             new TapDataType("char", "36", "uuid"))));
        assertEquals("VARCHAR2(128)",
                     mapper.getDataType(new ColumnDesc("t", "planeURI",
                             new TapDataType("char", "128*", "uri"))));
        assertEquals("SDO_GEOMETRY",
                     mapper.getDataType(new ColumnDesc("t", "position_bounds", TapDataType.POLYGON)));
        assertEquals("SDO_GEOMETRY",
                     mapper.getDataType(new ColumnDesc("t", "position_centroid", TapDataType.POINT)));
        assertEquals("VARCHAR2(128)",
                     mapper.getDataType(new ColumnDesc("t", "energy_bounds", TapDataType.INTERVAL)));
        assertEquals("TIMESTAMP",
                     mapper.getDataType(new ColumnDesc("t", "lastModified", TapDataType.TIMESTAMP)));
    }

    @Test
    public void getIntervalObjectFormatsDaliInterval() {
        final OracleDataType mapper = new OracleDataType();
        final Interval<Double> interval = new Interval<>(1.174e-6, 1.332e-6);

        assertEquals(new DoubleIntervalFormat().format(interval),
                     mapper.getIntervalObject(new DoubleInterval(interval.getLower(), interval.getUpper())));
    }

    @Test
    public void getPointObjectReturnsDaliPoint() {
        final OracleDataType mapper = new OracleDataType();
        final Point point = new Point(19.32, -34.15);

        assertSame(point, mapper.getPointObject(point));
    }

    @Test
    public void getPolygonObjectReturnsDaliPolygon() {
        final OracleDataType testSubject = new OracleDataType();
        final Polygon polygon = new Polygon();
        polygon.getVertices().add(new Point(1.0D, 2.0D));
        polygon.getVertices().add(new Point(3.0D, 4.0D));
        polygon.getVertices().add(new Point(5.0D, 6.0D));

        assertSame(polygon, testSubject.getPolygonObject(polygon));
    }
}
