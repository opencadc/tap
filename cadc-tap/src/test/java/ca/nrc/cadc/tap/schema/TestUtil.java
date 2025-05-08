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
package ca.nrc.cadc.tap.schema;

import java.util.ArrayList;

import java.util.List;

/**
 * Utility class solely for the purpose of testing.
 * 
 * @author Sailor Zhang
 *
 */
public class TestUtil
{
    
    public static TapSchema createSimpleTapSchema(int tapVersion, int numSchemas, int numTables, int numColumns, int numFunctions)
    {
        TapSchema ret = new TapSchema();
        for (int s=0; s<numSchemas; s++)
        {
            String sn = "schema" + s;
            SchemaDesc sd = new SchemaDesc(sn);
            
            for (int t=0; t<numTables; t++)
            {
                String tn = "table" + t;
                TableDesc td = new TableDesc(sn, tn);
                td.description = "this is " + tn;
                
                for (int c=0; c<numColumns; c++)
                {
                    
                    String cn = "c_int" + c;
                    String asz = null;
                    if (c > 0)
                        asz = Integer.toString(c);
                    if (tapVersion == 10)
                        td.getColumnDescs().add(new ColumnDesc(tn, cn, new TapDataType("adql:INTEGER", asz, null)));
                    else
                        td.getColumnDescs().add(new ColumnDesc(tn, cn, new TapDataType("int", asz, null)));
                    
                    cn = "c_double" + c;
                    asz = null;
                    if (tapVersion == 10)
                        td.getColumnDescs().add(new ColumnDesc(tn, cn, new TapDataType("adql:DOUBLE", asz, null)));
                    else
                        td.getColumnDescs().add(new ColumnDesc(tn, cn, new TapDataType("double", asz, null)));
                    
                    cn = "c_char" + c;
                    asz = null;
                    if (tapVersion == 10)
                        td.getColumnDescs().add(new ColumnDesc(tn, cn, new TapDataType("adql:CHAR", asz, null)));
                    else
                        td.getColumnDescs().add(new ColumnDesc(tn, cn, new TapDataType("char", asz, null)));
                }
                String cn = "c_interval";
                
                ColumnDesc ci = new ColumnDesc(tn, cn, new TapDataType("double", "2", "interval"));
                ci.description = "interval column";
                td.getColumnDescs().add(ci);

                cn = "c_polygon";
                if (tapVersion == 10)
                {
                    ColumnDesc cp = new ColumnDesc(tn, cn, new TapDataType("adql:REGION", "*", null));
                    cp.description = "region column";
                    td.getColumnDescs().add(cp);
                }
                else
                {
                    ColumnDesc cp = new ColumnDesc(tn, cn, new TapDataType("double", "*", "polygon"));
                    cp.description = "polygon column";
                    td.getColumnDescs().add(cp);
                }
                
                // one column with the optional metadata
                cn = "c_ocd";
                ColumnDesc ocd = new ColumnDesc(tn, cn, new TapDataType("double", "64*", null));
                ocd.description = "something foo";
                ocd.ucd = "foo;bar";
                ocd.unit = "deg";
                ocd.utype = "foo:Bar.baz";
                ocd.columnID = "ref";
                td.getColumnDescs().add(ocd);
                
                sd.getTableDescs().add(td);
            }
            ret.getSchemaDescs().add(sd);
        }
        
        List<FunctionDesc> fds = new ArrayList<FunctionDesc>();
        for (int f=0; f<numFunctions; f++)
        {
            String s = "func" + f;
            FunctionDesc fd = new FunctionDesc(s, new TapDataType("double"));
        }
        
        return ret;
    }
}
