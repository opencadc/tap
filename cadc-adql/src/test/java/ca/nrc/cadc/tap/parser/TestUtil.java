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

/**
 * 
 */
package ca.nrc.cadc.tap.parser;

import java.util.ArrayList;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.KeyColumnDesc;
import ca.nrc.cadc.tap.schema.KeyDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;

/**
 * Utility class solely for the purpose of testing.
 * 
 * @author Sailor Zhang
 *
 */
public class TestUtil
{
    /**
     * load a TAP Schema for test purpose.
     * 
     */
    public static TapSchema loadDefaultTapSchema()
    {
        return mockTapSchema();
    }

    
    static ColumnDesc createColumnDesc(String tn, String cn, TapDataType tt,
            String desc, String utype, String ucd, String unit)
    {
        ColumnDesc ret = new ColumnDesc(tn, cn, tt);
        ret.description = desc;
        ret.utype = utype;
        ret.ucd = ucd;
        ret.unit = unit;
        return ret;
    }
    
    /**
     * @return a mocked TAP schema
     */
    public static TapSchema mockTapSchema()
    {
        TapSchema ts = new TapSchema();

        String sn = "tap_schema";
        SchemaDesc sd = new SchemaDesc(sn);
        ts.getSchemaDescs().add(sd);

        String tn;
        TableDesc td;

        // custom test table in tap_schema
        tn = sn + ".alldatatypes";
        td = new TableDesc(sn, tn);
        sd.getTableDescs().add(td);
        td.getColumnDescs().add( createColumnDesc(tn, "t_integer", TapDataType.INTEGER, "int column", null, null, null));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_long", TapDataType.LONG));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_float", TapDataType.FLOAT));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_double", TapDataType.DOUBLE));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_char", new TapDataType("char", "8", null)));
        td.getColumnDescs().add( createColumnDesc(tn, "t_varchar", new TapDataType("char", "8*", null), "varchar column", null, null, null));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_string", new TapDataType("char", "8*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_bytes", TapDataType.BLOB));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_text", TapDataType.CLOB));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_point", TapDataType.POINT));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_region", TapDataType.POLYGON));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_timestamp", TapDataType.TIMESTAMP));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_int_array", new TapDataType("int", "2", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "t_double_array", new TapDataType("double", "2", null)));
        td.getColumnDescs().add( createColumnDesc(tn, "t_complete", new TapDataType("double", "2", null), "column with full metadata", "test:come.data.model","meta.ucd", "m") );


        // standard minimal self-describing tap_schema tables
        tn = sn + ".tables";
        td = new TableDesc(sn, tn);
        sd.getTableDescs().add(td);
        td.getColumnDescs().add( new ColumnDesc(tn, "schema_name", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "table_name", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "utype", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "description", new TapDataType("char", "16*", null)));
        KeyDesc k = new KeyDesc("k1", "TAP_SCHEMA.tables", "TAP_SCHEMA.schemas");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k1", "schema_name", "schema_name"));
        td.getKeyDescs().add(k);


        tn = sn + ".columns";
        td = new TableDesc(sn, tn);
        sd.getTableDescs().add(td);
        td.getColumnDescs().add( new ColumnDesc(tn, "table_name", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "column_name", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "utype", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "ucd", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "unit", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "description", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "datatype", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "arraysize", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "xtype", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "\"size\"", TapDataType.INTEGER));
        td.getColumnDescs().add( new ColumnDesc(tn, "principal", TapDataType.INTEGER));
        td.getColumnDescs().add( new ColumnDesc(tn, "indexed", TapDataType.INTEGER));
        td.getColumnDescs().add( new ColumnDesc(tn, "std", TapDataType.INTEGER));
        k = new KeyDesc("k2", "TAP_SCHEMA.columns", "TAP_SCHEMA.tables");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k2", "table_name", "table_name"));
        td.getKeyDescs().add(k);


        tn = sn + ".keys";
        td = new TableDesc(sn, tn);
        sd.getTableDescs().add(td);
        td.getColumnDescs().add( new ColumnDesc(tn, "key_id", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "from_table", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "target_table", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "utype", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "description", new TapDataType("char", "16*", null)));
        k = new KeyDesc("k3", "TAP_SCHEMA.keys", "TAP_SCHEMA.tables");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k3", "from_table", "table_name"));
        td.getKeyDescs().add(k);
        k = new KeyDesc("k4", "TAP_SCHEMA.keys", "TAP_SCHEMA.tables");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k4", "target_table", "table_name"));
        td.getKeyDescs().add(k);


        tn = sn + ".key_columns";
        td = new TableDesc(sn, tn);
        sd.getTableDescs().add(td);
        td.getColumnDescs().add( new ColumnDesc(tn, "key_id", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "from_column", new TapDataType("char", "16*", null)));
        td.getColumnDescs().add( new ColumnDesc(tn, "target_column", new TapDataType("char", "16*", null)));
        k = new KeyDesc("k5", "TAP_SCHEMA.key_columns", "TAP_SCHEMA.keys");
        k.getKeyColumnDescs().add(new KeyColumnDesc("k5", "key_id", "key_id"));
        td.getKeyDescs().add(k);

        sn = "caom";
        sd = new SchemaDesc(sn);
        ts.getSchemaDescs().add(sd);

        tn = sn + ".siav1";
        td = new TableDesc(sn, tn);
        sd.getTableDescs().add(td);
        td.getColumnDescs().add( new ColumnDesc(tn, "position_center_ra", TapDataType.DOUBLE));
        td.getColumnDescs().add( new ColumnDesc(tn, "position_center_dec", TapDataType.DOUBLE));

        ts.getFunctionDescs().add(new FunctionDesc("AREA", TapDataType.DOUBLE));
        ts.getFunctionDescs().add(new FunctionDesc("AVG", TapDataType.FUNCTION_ARG));
        ts.getFunctionDescs().add(new FunctionDesc("CIRCLE", TapDataType.DOUBLE));
        ts.getFunctionDescs().add(new FunctionDesc("COUNT", TapDataType.LONG));
        ts.getFunctionDescs().add(new FunctionDesc("MAX", TapDataType.FUNCTION_ARG));
        ts.getFunctionDescs().add(new FunctionDesc("MIN", TapDataType.FUNCTION_ARG));
        ts.getFunctionDescs().add(new FunctionDesc("STDDEV", TapDataType.DOUBLE));
        ts.getFunctionDescs().add(new FunctionDesc("SUM", TapDataType.FUNCTION_ARG));
        ts.getFunctionDescs().add(new FunctionDesc("VARIANCE", TapDataType.DOUBLE));

        return ts;
    }

    public static String getCallingMethod() {
        return trace(Thread.currentThread().getStackTrace(), 2);
    }
 
    public static String getCallingMethod(int level) {
        return trace(Thread.currentThread().getStackTrace(), 2 + level);
    }
 
    private static String trace(StackTraceElement e[], int level) {
        String rtn=null;
        if(e != null && e.length >= level) {
            StackTraceElement s = e[level];
            if(s != null) 
                rtn = s.getMethodName();
        }
        return rtn;
    }
}
