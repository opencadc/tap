/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

package ca.nrc.cadc.tap.schema;

import ca.nrc.cadc.dali.tables.votable.VOTableUtil;
import org.apache.log4j.Logger;

/**
 * TAP data type descriptor.
 *
 * @author pdowler
 */
public class TapDataType {
    private static final Logger log = Logger.getLogger(TapDataType.class);

    private String datatype;
    public String arraysize;
    public String xtype;

    public TapDataType(String datatype) {
        TapSchema.assertNotNull(TapDataType.class, "datatype", datatype);
        this.datatype = datatype;
    }

    public TapDataType(String datatype, String arraysize, String xtype) {
        this(datatype);
        this.arraysize = arraysize;
        this.xtype = xtype;
    }

    public String getDatatype() {
        return datatype;
    }

    public boolean isVarSize() {
        return (arraysize != null && arraysize.indexOf('*') >= 0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TapDataType[").append(datatype).append(",");
        sb.append(arraysize).append(",");
        sb.append(xtype).append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof TapDataType) {
            TapDataType rhs = (TapDataType) obj;
            if (!datatype.equals(rhs.datatype)) {
                return false;
            }

            if (xtype == null && rhs.xtype != null) {
                return false;
            }
            if (xtype != null && rhs.xtype == null) {
                return false;
            }
            if (xtype != null && !xtype.equals(rhs.xtype)) {
                return false;
            }
            // both xtypes null

            if (arraysize == null && rhs.arraysize == null) {
                return true; // scalar
            }
            if (arraysize == null && rhs.arraysize != null) {
                return false;
            }
            if (arraysize != null && rhs.arraysize == null) {
                return false;
            }
            // both arraysize not null

            int[] shape = VOTableUtil.getArrayShape(arraysize);
            int[] rshape = VOTableUtil.getArrayShape(rhs.arraysize);
            if (shape.length != rshape.length) {
                return false;
            }
            for (int i = 0; i < shape.length; i++) {
                if (shape[i] == -1 || rshape[i] == -1) { 
                    // variable ~true
                    return true;
                }
                if (shape[i] != rshape[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public static final TapDataType FUNCTION_ARG = new TapDataType("function-arg", null, null);

    // VOTable primitive types
    public static final TapDataType BOOLEAN = new TapDataType("boolean");
    public static final TapDataType SHORT = new TapDataType("short");
    public static final TapDataType INTEGER = new TapDataType("int");
    public static final TapDataType LONG = new TapDataType("long");
    public static final TapDataType FLOAT = new TapDataType("float");
    public static final TapDataType DOUBLE = new TapDataType("double");
    public static final TapDataType CHAR = new TapDataType("char");

    public static final TapDataType STRING = new TapDataType("char", "*", null);

    // DALI types
    public static final TapDataType TIMESTAMP = new TapDataType("char", "*", "timestamp");
    public static final TapDataType INTERVAL = new TapDataType("double", "2", "interval");
    public static final TapDataType POINT = new TapDataType("double", "2", "point");
    public static final TapDataType CIRCLE = new TapDataType("double", "3", "circle");
    public static final TapDataType POLYGON = new TapDataType("double", "*", "polygon");

    // DALI prototypes
    public static final TapDataType MULTIPOLYGON = new TapDataType("char", "*", "multipolygon");
    public static final TapDataType SHAPE = new TapDataType("char", "*", "shape");
    public static final TapDataType URI = new TapDataType("char", "*", "uri");

    // ADQL types
    public static final TapDataType BLOB = new TapDataType("byte", "*", "blob");
    public static final TapDataType CLOB = new TapDataType("char", "*", "clob");
}
