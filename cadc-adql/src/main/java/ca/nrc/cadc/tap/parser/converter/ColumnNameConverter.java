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

package ca.nrc.cadc.tap.parser.converter;

import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.log4j.Logger;

/**
 * Simple class to map columns name(s) used in the query to column name(s) used
 * in the database.
 *
 * @author zhangsa
 */
public class ColumnNameConverter extends ReferenceNavigator {

    protected static Logger log = Logger.getLogger(ColumnNameConverter.class);

    public Map<QualifiedColumn, QualifiedColumn> map;
    private final TapSchema tapSchema;

    public static class QualifiedColumn {

        String tableName;
        String columnName;

        public QualifiedColumn(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public String toString() {
            return "QualifiedColumn[" + tableName + "," + columnName + ']';
        }
        
    }

    private static class QualfiedColumnComparator implements Comparator<QualifiedColumn> {

        private boolean ignoreCase;

        public QualfiedColumnComparator(boolean ignoreCase) {
            this.ignoreCase = ignoreCase;
        }

        @Override
        public int compare(QualifiedColumn lhs, QualifiedColumn rhs) {
            int ret;
            if (lhs.tableName == null || rhs.tableName == null) {
                if (lhs.tableName == null && rhs.tableName == null) {
                    ret = 0;
                } else if (lhs.tableName == null) {
                    ret = -1; // null after not null
                } else {
                    ret = 1;
                }
            } else {
                if (ignoreCase) {
                    ret = lhs.tableName.compareToIgnoreCase(rhs.tableName);
                } else {
                    ret = lhs.tableName.compareTo(rhs.tableName);
                }
            }

            if (ret == 0) {
                if (ignoreCase) {
                    ret = lhs.columnName.compareToIgnoreCase(rhs.columnName);
                } else {
                    ret = lhs.columnName.compareTo(rhs.columnName);
                }
            }
            return ret;
        }

    }

    public ColumnNameConverter(boolean ignoreCase, TapSchema tapSchema) {
        this.map = new TreeMap<QualifiedColumn, QualifiedColumn>(new QualfiedColumnComparator(ignoreCase));
        this.tapSchema = tapSchema;
    }

    /**
     * Add new entries to the column name map.
     *
     * @param originalName a column name that should be replaced
     * @param newName the value that originalName should be replaced with
     * @deprecated use put(QualifiedColumn, QualifiedColumn)
     */
    @Deprecated
    public void put(String originalName, String newName) {
        String t = findTableNameInTapSchema(originalName);
        map.put(new QualifiedColumn(t, originalName), new QualifiedColumn(t, newName));
    }

    /**
     * Add new entries to the column name map.
     *
     * @param originalName a column name that should be replaced
     * @param newName the value that originalName should be replaced with
     */
    public void put(QualifiedColumn originalName, QualifiedColumn newName) {
        map.put(originalName, newName);
    }

    /* (non-Javadoc)
     * @see net.sf.jsqlparser.statement.select.ColumnReferenceVisitor#visit(net.sf.jsqlparser.schema.Column)
     */
    @Override
    public void visit(Column column) {
        log.debug("visit(column) " + column);
        String tableName = null;
        TableDesc td = TapSchemaUtil.findTableDesc(tapSchema, column.getTable());
        if (td != null) {
            tableName = td.getTableName();
        } else {
            tableName = findTableName(column);
        }
        
        log.debug("visit(Column) : tableName=" + tableName);
        String columnName = column.getColumnName();
        QualifiedColumn qc = new QualifiedColumn(tableName, columnName);
        QualifiedColumn nqc = map.get(qc);
        log.debug("visit(Column) : " + qc + " :: " + nqc);
        if (nqc != null) {
            column.setColumnName(nqc.columnName);
        }
    }
    
    private String findTableName(Column col) {
        String ret = null;
        Table t = TapSchemaUtil.findTableForColumnName(tapSchema, super.getSelectNavigator().getPlainSelect(), col);
        if (t != null) {
            TableDesc td = TapSchemaUtil.findTableDesc(tapSchema, t);
            if (td != null) {
                ret = td.getTableName();
            }
        }
        return ret;
    }
    
    private String findTableNameInTapSchema(String col) {
        String ret = null;
        for (SchemaDesc sd : tapSchema.getSchemaDescs()) {
            for (TableDesc td : sd.getTableDescs()) {
                for (ColumnDesc cd : td.getColumnDescs()) {
                    if (cd.getColumnName().equalsIgnoreCase(col)) {
                        if (ret != null) {
                            throw new IllegalArgumentException("ambiguous unqualified column: " + col);
                        }
                        ret = td.getTableName();
                    }
                }
            }
        }
        return ret;
    }
}
