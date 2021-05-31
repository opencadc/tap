
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

package ca.nrc.cadc.tap.writer.format;

import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.tap.parser.region.function.OracleCircle;
import ca.nrc.cadc.tap.parser.region.function.OracleGeometricFunction;
import ca.nrc.cadc.tap.parser.region.function.OraclePolygon;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;

import org.apache.log4j.Logger;


public class OracleRegionFormat extends AbstractResultSetFormat {

    private static final Logger LOGGER = Logger.getLogger(OracleRegionFormat.class);
    private static final int EXPECTED_ARGUMENT_LENGTH = 5;

    @Override
    public Object extract(ResultSet resultSet, int i) throws SQLException {
        return format(resultSet.getObject(i));
    }

    @Override
    public String format(final Object object) {
        final String returnValue;

        if (object == null) {
            returnValue = null;
        } else if (object instanceof Struct) {
            final Struct struct = (Struct) object;
            returnValue = toString(struct);
        } else {
            returnValue = object.toString();
        }

        return returnValue;
    }

    private String toString(final Struct struct) {
        if (struct == null) {
            return null;
        } else {
            try {
                final String functionName = struct.getSQLTypeName();
                final Object[] functionAttributes = struct.getAttributes();

                if (functionAttributes.length != EXPECTED_ARGUMENT_LENGTH
                    || !functionName.toUpperCase().contains(OracleGeometricFunction.ORACLE_GEOMETRY_FUNCTION_NAME)) {
                    throw new IllegalArgumentException(
                            String.format("Invalid Region function found '%s'", functionName));
                }

                final BigDecimal pointOrPolygonType = (BigDecimal) functionAttributes[0];
                final int typeValue = pointOrPolygonType.intValue();

                LOGGER.debug("Function name: " + functionName + ".");
                LOGGER.debug("Function attributes: " + Arrays.toString(functionAttributes) + ".");
                LOGGER.debug("Function type value: " + typeValue);

                // Circle, Polygon, or Union.
                if (typeValue == OracleGeometricFunction.POLYGON_GEO_TYPE) {
                    return polygonToString(toBigDecimalArray(functionAttributes[3]),
                                           toBigDecimalArray(functionAttributes[4]));
                } else if (typeValue == OracleGeometricFunction.POINT_GEO_TYPE) {
                    return pointToString(toBigDecimalArray(functionAttributes[2]));
                } else if (typeValue == OracleGeometricFunction.UNION_GEO_TYPE) {
                    return "UNION (In progress)";
                } else {
                    throw new IllegalArgumentException(
                            String.format("Unsupported Region function '%s'.", functionName));
                }
            } catch (ClassCastException cce) {
                return String.format("Invalid format for the Region from Oracle.\n\n%s\n", cce.getMessage());
            } catch (SQLException e) {
                return String.format("Unexpected Region value.\n\n%s\n", e.getMessage());
            }
        }
    }

    String polygonToString(final BigDecimal[] structTypeArray, final BigDecimal[] structVerticesArray) {
        // For a 3-length vertice structure, it's a Circle.
        if (structVerticesArray.length == 3) {
            return new OracleCircleFormat().format(structVerticesArray);
        } else if (OraclePolygon.structMatches(structTypeArray)) {
            return new OraclePolygonFormat().format(structVerticesArray);
        } else {
            return null;
        }
    }

    String pointToString(final BigDecimal[] structAttributes) {
        final OraclePointFormat oraclePointFormat = new OraclePointFormat();
        return oraclePointFormat.format(
                new Point(structAttributes[0].doubleValue(), structAttributes[1].doubleValue()));
    }

    BigDecimal[] toBigDecimalArray(final Object structArray) throws SQLException {
        if (structArray instanceof Array) {
            return (BigDecimal[]) ((Array) structArray).getArray();
        } else {
            return null;
        }
    }
}
