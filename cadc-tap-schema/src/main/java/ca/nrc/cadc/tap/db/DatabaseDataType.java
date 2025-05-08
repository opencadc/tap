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
*  $Revision: 4 $
*
************************************************************************
 */

package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TapDataType;

/**
 * Interface to convert ADQL data types to a database
 * specific data types.
 *
 * @author jburke
 */
public interface DatabaseDataType {
    /**
     * Get the database type for the specified column. This is for use in create
     * table statements, casts, etc.
     *
     * @param columnDesc ADQL description of the column
     * @return database specific data type
     */
    String getDataType(ColumnDesc columnDesc);

    /**
     * Get the column type as a java.sql.Types constant.
     *
     * @param columnDesc
     * @return one of the java.sql.Types values
     */
    Integer getType(ColumnDesc columnDesc);

    /**
     * Convert a database data type to a a TAP data type. This is only used by
     * the TableIngester to read a database table and create a TAP table description
     * of it.
     *
     * @param datatype the database data type
     * @param length length of the column or null
     * @return a TapDataType
     */
    TapDataType toTapDataType(String datatype, Integer length);

    /**
     * Convert the argument database object name (schema, table, column)
     * into a suitable internal representation for use with the JDBC
     * DatabaseMetadata API. For example, in postgresql this method would
     * convert the argument to lower case because that database backend uses
     * lower case internally. This is only used by the TableIngester to find
     * and read table structure from the database.
     *
     * @param name a schema|table|column name
     * @return internal name
     */
    String toInternalDatabaseObjectName(String name);

    /**
     * Get an optional USING qualifier for index creation.
     *
     * @param columnDesc
     * @param unique
     * @return USING qualifier or null if not applicable
     * @throws IllegalArgumentException if unique==true and the column type or qualifier
     *         does not support unique indices
     */
    String getIndexUsingQualifier(ColumnDesc columnDesc, boolean unique);

    /**
     * Get an optional operator for index creation. If you don't know what this
     * is just return null.
     *
     * @param columnDesc
     * @return
     */
    String getIndexColumnOperator(ColumnDesc columnDesc);

    /**
     * Convert TAP-1.0 ADQL/STC region value to a database object for insert.
     *
     * @param reg
     * @return
     */
    @Deprecated
    Object getRegionObject(ca.nrc.cadc.stc.Region reg);

    /**
     * Convert TAP-1.0 ADQL/STC point value to a database object for insert.
     *
     * @param pos
     * @return
     */
    @Deprecated
    Object getPointObject(ca.nrc.cadc.stc.Position pos);

    /**
     * Convert DALI-1.1 point to a database object for insert.
     *
     * @param p
     * @return
     */
    Object getPointObject(Point p);

    /**
     * Convert DALI-1.1 circle to a database object for insert.
     *
     * @param c
     * @return
     */
    Object getCircleObject(Circle c);

    /**
     * Convert DALI-1.1 polygon to a database object for insert.
     *
     * @param poly
     * @return
     */
    Object getPolygonObject(Polygon poly);

    /**
     * Convert DALI-1.1 interval to a database object for insert.
     *
     * @param inter
     * @return
     */
    Object getIntervalObject(DoubleInterval inter);

    /**
     * Convert an array of DALI-1.1 intervals to a database object for insert.
     *
     * @param inter
     * @return
     */
    Object getIntervalArrayObject(DoubleInterval[] inter);

    Object getArrayObject(short[] val);

    Object getArrayObject(int[] val);

    Object getArrayObject(long[] val);

    Object getArrayObject(float[] val);

    Object getArrayObject(double[] val);

}
