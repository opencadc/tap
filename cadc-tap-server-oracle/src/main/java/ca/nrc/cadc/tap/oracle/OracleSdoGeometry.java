
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
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.oracle;

import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.tap.parser.region.function.OracleGeometricFunction;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;

import oracle.jdbc.OracleConnection;


/**
 * Build {@code MDSYS.SDO_GEOMETRY} values for JDBC insert/update.
 */
public final class OracleSdoGeometry {

    static final String SDO_GEOMETRY_TYPE = "MDSYS.SDO_GEOMETRY";

    static final String SDO_POINT_TYPE = "MDSYS.SDO_POINT_TYPE";

    private static final String SDO_ELEM_INFO_ARRAY_TYPE = "MDSYS.SDO_ELEM_INFO_ARRAY";
    private static final String SDO_ORDINATE_ARRAY_TYPE = "MDSYS.SDO_ORDINATE_ARRAY";

    private static final int[] POLYGON_ELEM_INFO = new int[] { 1, 2003, 1 };

    private OracleSdoGeometry() {
    }

    /**
     * Materialize a DALI point as {@code MDSYS.SDO_GEOMETRY}.
     */
    public static Struct createPoint(Connection connection, Point point) throws SQLException {
        if (point == null) {
            return null;
        }

        Object[] pointAttributes = new Object[] {
            BigDecimal.valueOf(point.getLongitude()),
            BigDecimal.valueOf(point.getLatitude()),
            null
        };
        OracleConnection oracleConnection = asOracleConnection(connection);
        Struct sdoPoint = oracleConnection.createStruct(SDO_POINT_TYPE, pointAttributes);

        Object[] attributes = new Object[] {
            BigDecimal.valueOf(OracleGeometricFunction.POINT_GEO_TYPE),
            BigDecimal.valueOf(OracleGeometricFunction.SRID_VALUE),
            sdoPoint,
            null,
            null
        };

        return OracleSdoGeometry.asOracleConnection(connection).createStruct(SDO_GEOMETRY_TYPE, attributes);
    }

    /**
   * Materialize a DALI polygon as {@code MDSYS.SDO_GEOMETRY}.
   *
   * @param connection JDBC connection used to create structs and arrays
   * @param poly polygon in degrees (lon/lat), may be null
   * @return {@link Struct} or null
   */
    public static Struct createPolygon(Connection connection, Polygon poly) throws SQLException {
        if (poly == null) {
            return null;
        }

        BigDecimal[] elemInfo = new BigDecimal[POLYGON_ELEM_INFO.length];
        for (int i = 0; i < POLYGON_ELEM_INFO.length; i++) {
            elemInfo[i] = BigDecimal.valueOf(POLYGON_ELEM_INFO[i]);
        }

        BigDecimal[] ordinates = new BigDecimal[poly.getVertices().size() * 2];
        int ordinateIndex = 0;
        for (Point p : poly.getVertices()) {
            ordinates[ordinateIndex++] = BigDecimal.valueOf(p.getLongitude());
            ordinates[ordinateIndex++] = BigDecimal.valueOf(p.getLatitude());
        }

        Array elemInfoArray = createNamedArray(connection, SDO_ELEM_INFO_ARRAY_TYPE, elemInfo);
        Array ordinatesArray = createNamedArray(connection, SDO_ORDINATE_ARRAY_TYPE, ordinates);

        Object[] attributes = new Object[] {
            BigDecimal.valueOf(OracleGeometricFunction.POLYGON_GEO_TYPE),
            BigDecimal.valueOf(OracleGeometricFunction.SRID_VALUE),
            null,
            elemInfoArray,
            ordinatesArray
        };

        return OracleSdoGeometry.asOracleConnection(connection).createStruct(SDO_GEOMETRY_TYPE, attributes);
    }

    /**
     * Oracle VARRAY types ({@code SDO_*_ARRAY}) require {@code OracleConnection.createOracleArray},
     * not JDBC {@code Connection.createArrayOf}. Object structs likewise need an unwrapped connection.
     *
     * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/Oracle-collections.html"
     *     >Oracle collections</a>
     */
    private static Array createNamedArray(Connection connection, String sqlTypeName, Object elements)
            throws SQLException {
        return OracleSdoGeometry.asOracleConnection(connection).createOracleArray(sqlTypeName, elements);
    }

    private static OracleConnection asOracleConnection(Connection connection) throws SQLException {
        if (!connection.isWrapperFor(OracleConnection.class)) {
            throw new SQLException("Not an Oracle JDBC connection: " + connection.getClass().getName());
        }
        return connection.unwrap(OracleConnection.class);
    }
}
