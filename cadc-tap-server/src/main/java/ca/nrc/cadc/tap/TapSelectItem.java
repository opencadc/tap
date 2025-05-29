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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;

/**
 * Local replacement for ParamDesc in cadc-tap-schema library.
 *
 * @author pdowler
 */
public class TapSelectItem {
    private String name;
    private TapDataType datatype;

    private String columnName;
    public String tableName;

    public String description;
    public String utype;
    public String ucd;
    public String unit;
    public boolean principal;
    public boolean indexed;
    public boolean std;
    public String columnID;

    /**
     * A normal column with an alternate name (alias).
     * All metadata is copied from the specified column descriptor.
     *
     * @param name
     * @param column
     */
    public TapSelectItem(String name, ColumnDesc column) {
        this(name, column.getDatatype());
        this.columnName = column.getColumnName();
        this.tableName = column.getTableName();
        this.description = column.description;
        this.columnID = column.columnID;
        this.indexed = column.indexed;
        this.principal = column.principal;
        this.std = column.std;
        this.ucd = column.ucd;
        this.unit = column.unit;
        this.utype = column.utype;
    }

    public String getName() {
        return name;
    }

    public TapDataType getDatatype() {
        return datatype;
    }

    /**
     * Original column name if the selected item was a column.
     *
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * A new column created by some sort of expression. This could be a function call, algebraic
     * expression, case statement, etc. The calling code must set any additional column
     * metadata.
     *
     * @param name
     * @param datatype
     */
    public TapSelectItem(String name, TapDataType datatype) {
        TapSchema.assertNotNull(TapSelectItem.class, "name", name);
        TapSchema.assertNotNull(TapSelectItem.class, "datatype", datatype);
        this.name = name;
        this.datatype = datatype;
    }

    @Override
    public String toString() {
        return "TapSelectItem[" + name + "," + datatype + "]";
    }

}
