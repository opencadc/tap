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
************************************************************************
 */

package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.tables.votable.VOTableUtil;
import ca.nrc.cadc.stc.Position;
import ca.nrc.cadc.stc.Region;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class BasicDataTypeMapper implements DatabaseDataType {

    private static final Logger log = Logger.getLogger(BasicDataTypeMapper.class);

    protected static class TypePair {

        /**
         * Column type for use in create table.
         */
        public String str;

        /**
         * Column type for use in PreparedStatement set methods.
         */
        Integer num;

        public TypePair(String s, Integer n) {
            this.str = s;
            this.num = n;
        }

        @Override
        public String toString() {
            return str + ":" + num;
        }
    }

    /**
     * Mapping of ADQL data types to PostgreSQL data types. Subclasses can (must)
     * add a mapping for optional types.
     */
    protected final Map<TapDataType, TypePair> dataTypes = new HashMap<>();

    /**
     * Mapping of database data types to VOTable data types.
     */
    protected final Map<String, TapDataType> dbDataTypes = new HashMap<>();

    public BasicDataTypeMapper() {
        // votable type -> db type
        dataTypes.put(TapDataType.BOOLEAN, new TypePair("BOOLEAN", Types.BOOLEAN));
        dataTypes.put(TapDataType.SHORT, new TypePair("SMALLINT", Types.SMALLINT));
        dataTypes.put(TapDataType.INTEGER, new TypePair("INTEGER", Types.INTEGER));
        dataTypes.put(TapDataType.LONG, new TypePair("BIGINT", Types.BIGINT));
        dataTypes.put(TapDataType.FLOAT, new TypePair("REAL", Types.REAL));
        dataTypes.put(TapDataType.DOUBLE, new TypePair("DOUBLE PRECISION", Types.DOUBLE));
        dataTypes.put(TapDataType.CHAR, new TypePair("CHAR", Types.CHAR));

        dataTypes.put(TapDataType.STRING, new TypePair("CHAR", Types.CHAR));
        dataTypes.put(TapDataType.TIMESTAMP, new TypePair("TIMESTAMP", Types.TIMESTAMP));
        dataTypes.put(TapDataType.URI, new TypePair("CHAR", Types.CHAR));

        // DatabaseMetadata -> TAP_DATA_TYPE
        // TYPE_NAME    DATA_TYPE   TAP_DATA_TYPE
        // bool         -7          BOOLEAN
        // bpchar       1           CHAR
        // varchar 4096 12          STRING or URI
        // int2         5           SHORT
        // int4         4           INTEGER
        // int8        -5           LONG
        // float4       7           FLOAT
        // float8       8           DOUBLE
        // timestamp    93          TIMESTAMP
        dbDataTypes.put("bool", TapDataType.BOOLEAN);
        dbDataTypes.put("int2", TapDataType.SHORT);
        dbDataTypes.put("int4", TapDataType.INTEGER);
        dbDataTypes.put("int8", TapDataType.LONG);
        dbDataTypes.put("float4", TapDataType.FLOAT);
        dbDataTypes.put("float8", TapDataType.DOUBLE);
        dbDataTypes.put("float8", TapDataType.DOUBLE);
        
        // TODO: bpchar is postgresql specific?
        dbDataTypes.put("bpchar", TapDataType.CHAR);                    // code to assign optional length
        
        dbDataTypes.put("char", TapDataType.CHAR);                      // code to assign optional length
        dbDataTypes.put("varchar", new TapDataType("char", "*", null)); // code to assign optional length
        dbDataTypes.put("text", new TapDataType("char", "*", null));    // code to assign optional length
        dbDataTypes.put("timestamp", TapDataType.TIMESTAMP);
    }

    /**
     * Get the column type for use in create table statements. This method uses the
     * dataType map and the findTypePair method so it should work for columns that match
     * a map key exactly plus arrays of primitives (e.g,. char(8)). It also handles making
     * variable sized columns for char (e.g. varchar) and imposes a limit when one is not
     * specified.
     *
     * @param columnDesc ADQL description of the column
     * @return datatype string for use in create table
     */
    @Override
    public String getDataType(ColumnDesc columnDesc) {
        TapDataType tt = columnDesc.getDatatype();
        TypePair dbt = findTypePair(tt);
        if (dbt == null) {
            throw new UnsupportedOperationException("unsupported database column type: " + tt);
        }

        String ret = dbt.str;
        if (ret.equals("CHAR")) {
            if (tt.isVarSize()) {
                ret = getVarCharType();
            }
            int[] arrayshape = VOTableUtil.getArrayShape(tt.arraysize);
            if (arrayshape != null && arrayshape[0] > 0) {
                ret += "(" + arrayshape[0] + ")";
            } else if (tt.isVarSize()) {
                ret += getDefaultCharlimit(); // HACK: arbitrary sensible limit
            }
        }

        log.debug("getDataType (return): " + columnDesc + " -> " + ret);
        return ret;
    }

    /**
     *
     * @param columnDesc
     * @return data type code for use in PreparedStatement set methods
     */
    @Override
    public Integer getType(ColumnDesc columnDesc) {
        TapDataType tt = columnDesc.getDatatype();
        TypePair dbt = findTypePair(tt);
        return dbt.num;
    }

    @Override
    public String getIndexUsingQualifier(ColumnDesc columnDesc, boolean unique) {
        return null;
    }

    @Override
    public String getIndexColumnOperator(ColumnDesc columnDesc) {
        return null;
    }

    /**
     * Maps standard database datatypes to a TapDatatype. Database specific datatypes 
     * can usually map custom types by adding entries to the dataTypes and dbDataTypes
     * maps and letting this method work it out, but they may need to override
     * this in some niche cases.
     *
     * @param datatype database datatype
     * @param length length of the column, possibly null
     * @return TapDatatype
     */
    @Override
    public TapDataType toTapDataType(String datatype, Integer length) {
        TapDataType ret = dbDataTypes.get(datatype);
        if (length != null) {
            String as = length.toString();
            if (ret.isVarSize()) {
                as += "*";
            }
            ret = new TapDataType(ret.getDatatype(), as, null); // only for char(N) and varchar(N)
        }
        if (ret != null) {
            return ret;
        }
        throw new UnsupportedOperationException("Unknown database datatype: " + datatype);
    }

    /**
     * Default implementation: return the name as is.
     * @param name of a schema|table|column
     * @return argument name unchanged
     */
    @Override
    public String toInternalDatabaseObjectName(String name) {
        return name;
    }

    
    /**
     * Find or create a TypePair for the specified data type. The current implementation
     * looks for exact matches in the dataTypes map and, if not found, it rechecks with
     * just the base datatype when the specified TapDataType has length is greater than
     * 1; the latter takes care of arrays of strings (char(n) or char(*)) and should work
     * for other arrays.
     *
     * @param tt
     * @return
     */
    protected TypePair findTypePair(TapDataType tt) {
        TypePair dbt = dataTypes.get(tt);
        if (dbt == null && tt.arraysize != null) {
            // input may have a more restrictive arraysize
            TapDataType tmp = new TapDataType(tt.getDatatype(), "*", tt.xtype);
            dbt = dataTypes.get(tmp);
        }
        if (dbt == null) {
            throw new UnsupportedOperationException("unexpected datatype: " + tt);
        }

        log.debug("findTypePair: " + tt + " -> " + dbt);
        return dbt;
    }

    /**
     * Return the default quantifier (length of char or varchar columns) when none is
     * specified. The return must include the correct form of braces in addition to the
     * size. The default return value is <code>(4096)</code>.
     *
     * @return
     */
    protected String getDefaultCharlimit() {
        return "(4096)";
    }

    /**
     * Return the database type to use for variable-length character columns. The default
     * return value is <code>VARCHAR</code>.
     *
     * @return
     */
    protected String getVarCharType() {
        return "VARCHAR";
    }

    @Override
    public Object getRegionObject(Region reg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getPointObject(Position pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getPointObject(Point p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getCircleObject(Circle c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getPolygonObject(Polygon poly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getIntervalObject(DoubleInterval inter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getIntervalArrayObject(DoubleInterval[] inter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArrayObject(short[] val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArrayObject(int[] val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArrayObject(long[] val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArrayObject(float[] val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getArrayObject(double[] val) {
        throw new UnsupportedOperationException();
    }
}
