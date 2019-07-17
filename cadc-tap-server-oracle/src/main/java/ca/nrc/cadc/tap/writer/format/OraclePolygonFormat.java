
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
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.util.PolygonFormat;
import ca.nrc.cadc.util.StringUtil;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class OraclePolygonFormat extends AbstractResultSetFormat {

    static final String POLYGON_FUNCTION = "SDO_ORDINATE_ARRAY";
    static final String SHAPE_NAME = "Polygon";
    private final PolygonFormat polygonFormat = new PolygonFormat();

    @Override
    public Object extract(final ResultSet resultSet, final int columnIndex) throws SQLException {
        final String s = resultSet.getString(columnIndex);
        return getPolygon(s);
    }

    @Override
    public String format(final Object object) {
        if (object instanceof Polygon) {
            return OraclePolygonFormat.SHAPE_NAME + " " + polygonFormat.format((Polygon) object);
        } else if (object instanceof BigDecimal[]) {
            return fromStruct((BigDecimal[]) object);
        } else {
            return object.toString();
        }
    }

    private String fromStruct(final BigDecimal[] structVerticeValues) {
        if (structVerticeValues.length < 6) {
            throw new IllegalArgumentException(
                    String.format("Should have at least six (6) values from the database, but has %d.",
                                  structVerticeValues.length));
        } else {
            final Polygon polygon = new Polygon();

            for (int i = 0; i < structVerticeValues.length; i = i+2) {
                final BigDecimal xPoint = structVerticeValues[i];
                final BigDecimal yPoint = structVerticeValues[i + 1];

                polygon.getVertices().add(new Point(xPoint.doubleValue(), yPoint.doubleValue()));
            }

            return format(polygon);
        }
    }


    Polygon getPolygon(final String clause) {
        final Polygon polygon = new Polygon();
        final Pattern p = Pattern.compile(OraclePolygonFormat.POLYGON_FUNCTION, Pattern.CASE_INSENSITIVE);
        final Matcher m = p.matcher(clause);

        if (m.find()) {
            final int openFunctionIndex = m.start();
            final int openFunctionParenIndex = clause.indexOf("(", openFunctionIndex);
            final int closeFunctionParenIndex = clause.indexOf(")", openFunctionParenIndex);
            final String functionArgumentString = clause.substring(openFunctionParenIndex + 1, closeFunctionParenIndex);

            polygon.getVertices().addAll(parsePoints(functionArgumentString));
        } else {
            throw new IllegalArgumentException(
                    String.format("Missing %s function type for Polygon clause '%s'",
                                  OraclePolygonFormat.POLYGON_FUNCTION,
                                  clause));
        }

        return polygon;
    }

    List<Point> parsePoints(final String s) {
        final String[] items = StringUtil.hasText(s) ? s.split(",") : new String[0];
        final int pointCount = items.length / 2;

        if (pointCount % 2 != 0 || pointCount < 3) {
            throw new IllegalArgumentException(
                    String.format(
                            "Array does not contain enough values (6 required) for an array of points (Found %d).",
                            items.length));
        } else {
            final List<Point> points = new ArrayList<>(pointCount);
            double[] longitudes = new double[pointCount];
            double[] latitudes = new double[pointCount];
            for (int i = 0, il = items.length; i < il; i++) {
                if (i % 2 == 0) {
                    longitudes[i / 2] = Double.parseDouble(items[i]);
                } else {
                    latitudes[(i - 1) / 2] = Double.parseDouble(items[i]);
                }
            }

            for (int i = 0, ll = longitudes.length; i < ll; i++) {
                points.add(new Point(longitudes[i], latitudes[i]));
            }
            return points;
        }
    }
}
