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

package ca.nrc.cadc.tap.parser.schema;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import org.apache.log4j.Logger;

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.exception.TapParserException;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import net.sf.jsqlparser.expression.Function;

/**
 * Utility class related to TAP Schema.
 * 
 * @author zhangsa
 *
 */
public class TapSchemaUtil
{
    protected static Logger log = Logger.getLogger(TapSchemaUtil.class);


    /**
     * For a given Table, find it in TAP Schema, and returns a list of ParamDesc of that Table.
     * 
     * @param tapSchema
     * @param table
     * @return
     * @throws TapParserException when the parse fails
     */
    public static List<ColumnDesc> getParamDescList(TapSchema tapSchema, Table table) throws TapParserException
    {
        TableDesc tableDesc = findTableDesc(tapSchema, table);
        if (tableDesc != null)
            return getParamDescList(tableDesc);
        else
            throw new TapParserException("Table: [" + table + "] does not exist.");
    }

    /**
     * Deep copy.
     * 
     * @param tableDesc
     * @return deep copy of the columns of a table
     */
    public static List<ColumnDesc> getParamDescList(TableDesc tableDesc)
    {
        List<ColumnDesc> list = new ArrayList<>();
        list.addAll(tableDesc.getColumnDescs());
        return list;
    }

    /**
     * Find TableDesc in TAP Schema for a given Table.
     * 
     * @param tapSchema
     * @param table
     * @return
     */
    public static TableDesc findTableDesc(TapSchema tapSchema, Table table)
    {
        if (table == null)
            return null;
        for (SchemaDesc sd : tapSchema.getSchemaDescs())
        {
            log.debug("findTableDesc: " + table.getWholeTableName() 
                    + " aka " + table.getSchemaName() + " + " + table.getName()
                    + " AS " + table.getAlias() + " vs " + sd.getSchemaName());
            
            if (table.getSchemaName() == null
                    || (sd.getSchemaName().equalsIgnoreCase(table.getSchemaName())))
            {
                for (TableDesc td : sd.getTableDescs())
                {
                    log.debug("findTableDesc: " + td.getTableName() + " vs " + table.getWholeTableName());
                    if (td.getTableName().equalsIgnoreCase(table.getWholeTableName()))
                    {
                        return td;
                    }
                }
            }
        }
        return null;
    }

    /**
     * For a columnName and a plainSelect, find the Table from TAP Schema.
     * 
     * @param tapSchema
     * @param plainSelect
     * @param columnName
     * @return
     */
    public static Table findTableForColumnName(TapSchema tapSchema, PlainSelect plainSelect, String columnName)
    {
        Table ret = null;
        int matchCount = 0;
        
        List<Table> fromTableList = ParserUtil.getFromTableList(plainSelect);
        for (Table fromTable : fromTableList)
        {
            TableDesc td = findTableDesc(tapSchema, fromTable);
            if (td == null) 
                throw new IllegalArgumentException("Table [" + fromTable + "] does not exist.");
            log.debug("findTableForColumnName: " + columnName + " in " + td.getTableName());
            if (isValidColumnName(td, columnName))
            {
                ret = fromTable;
                matchCount++;
            }
        }
        if (matchCount == 0)
            throw new IllegalArgumentException("findTableForColumnName: Column [" + columnName + "] does not exist.");
        else if (matchCount > 1) 
            throw new IllegalArgumentException("Column [" + columnName + "] is ambiguous.");

        log.debug("findTableForColumnName: found " + ret);
        return ret;
    }

    /**
     * Check whether a simple columnName exists in tableDesc
     *  
     * @param td
     * @param columnName
     * @return
     */
    private static boolean isValidColumnName(TableDesc td, String columnName)
    {
        ColumnDesc cd = findColumnDesc(td, columnName);
        return (cd != null);
    }
    
    /**
     * find ColumnDesc for given TableDesc and columnName.
     * 
     * @param td
     * @param columnName
     * @return
     */
    private static ColumnDesc findColumnDesc(TableDesc tableDesc, String columnName)
    {
        String scol = stripQuotes(columnName);
        List<ColumnDesc> cdList = tableDesc.getColumnDescs();
        for (ColumnDesc cd : cdList)
        {
            log.debug("findColumnDesc: " + cd.getColumnName() + " vs " + columnName + " " + scol);
            if (cd.getColumnName().equalsIgnoreCase(columnName) || cd.getColumnName().equalsIgnoreCase(scol))
            {
                return cd;
            }
            // support unquoted usage when tap_schema says it is quoted?
            String tsCol = stripQuotes(cd.getColumnName());
            if (tsCol.equalsIgnoreCase(columnName))
            {
                return cd;
            }
        }
        return null;
    }

    /**
     * get Table object of a TableDesc
     * 
     * @param rtnTd
     * @return
     */
    //private static Table getTable(TableDesc td)
    //{
    //    if (td == null)
    //        return null;
    //    return new Table(td.getSchemaName(), td.getTableName());
    //}

    private static String stripQuotes(String col)
    {
        if (col != null && col.length() > 1)
        {
            if (col.charAt(0) == '"' && col.charAt(col.length()-1) == '"')
                return col.substring(1, col.length()-1);
        }
        return col;
    }

    /**
     * Validate a column which is not an alias of selectItem.
     * 
     * Form of column could be possibly:
     * 
     * table.columnName, tableAilas.columnName, or schema.table.ColumnName
     * 
     * @param tapSchema
     * @param plainSelect
     * @param column
     */
    public static void validateColumnNonAlias(TapSchema tapSchema, PlainSelect plainSelect, Column column)
    {
        // columnName, table.columnName, tableAilas.columnName, or schema.table.ColumnName

        Table table = column.getTable();
        //String columnName = stripQuotes(column.getColumnName());
        String columnName = column.getColumnName();
        
        if (table == null || table.getName() == null || table.getName().equals(""))
        {
            // form: columnName
            Table fromTable = TapSchemaUtil.findTableForColumnName(tapSchema, plainSelect, columnName);
            if (fromTable == null) throw new IllegalArgumentException("validateColumnNonAlias: Column: [" + columnName + "] does not exist.");
        }
        else
        {
            // table.columnName, tableAilas.columnName, or schema.table.ColumnName
            String schemaName = table.getSchemaName();
            if (schemaName == null || schemaName.equals(""))
            {
                // table.columnName, tableAilas.columnName
                String tableNameOrAlias = table.getName();
                Table fromTable = ParserUtil.findFromTable(plainSelect, tableNameOrAlias);
                if (fromTable == null)
                    throw new IllegalArgumentException("Table: [" + tableNameOrAlias + "] not found in TapSchema");
                else
                {
                    TableDesc fromTableDesc = TapSchemaUtil.findTableDesc(tapSchema, fromTable);
                    if (!TapSchemaUtil.isValidColumnName(fromTableDesc, columnName))
                        throw new IllegalArgumentException("Column: [" + columnName + "] not found in TapSchema");
                }
            }
            else
            {
                // schema.table.ColumnName
                TableDesc fromTableDesc = TapSchemaUtil.findTableDesc(tapSchema, table);
                if (fromTableDesc == null)
                    throw new IllegalArgumentException("Table [ " + table + " ] not found in TapSchema");
                if (!TapSchemaUtil.isValidColumnName(fromTableDesc, columnName))
                    throw new IllegalArgumentException("Column: [" + columnName + "] not found in TapSchema");
            }
        }
    }

    /**
     * Return a list of SelectItem for a Table in a TAP schema.
     * 
     * @param tapSchema
     * @param table
     * @return
     */
    public static List<SelectItem> getSelectItemList(TapSchema tapSchema, Table table)
    {
        List<SelectItem> seiList = new ArrayList<SelectItem>();

        TableDesc td = findTableDesc(tapSchema, table);
        for (ColumnDesc cd : td.getColumnDescs())
        {
            SelectItem sei = TapSchemaUtil.newSelectExpressionItem(table, cd.getColumnName());
            seiList.add(sei);
        }
        return seiList;
    }

    /**
     * Return a SelectExpressionItem for a given Table and columnName
     * 
     * @param table
     * @param columnName
     * @return
     */
    private static SelectExpressionItem newSelectExpressionItem(Table table, String columnName)
    {
        Table siTable;
        String alias = table.getAlias();
        if (alias != null && !alias.isEmpty())
            siTable = new Table(null, alias);
        else
            siTable = table;
        Column column = new Column(siTable, columnName); 
        SelectExpressionItem sei = new SelectExpressionItem();
        sei.setExpression(column);
        return sei;
    }

    /**
     * Find ColumnDesc from TAP Schema for given Column and plainSelect.
     * 
     * @param tapSchema
     * @param plainSelect
     * @param column
     * @return
     */
    public static ColumnDesc findColumnDesc(TapSchema tapSchema, PlainSelect plainSelect, Column column)
    {
        // Possible form as:
        // columnName, table.columnName, tableAilas.columnName, or schema.table.ColumnName
        Table qualifiedTable = getQualifiedTable(tapSchema, plainSelect, column);
        log.debug("findColumnDesc: found qualified " + qualifiedTable);
        TableDesc tableDesc = findTableDesc(tapSchema, qualifiedTable);
        log.debug("findColumnDesc: found tableDesc " + tableDesc);
        return findColumnDesc(tableDesc, column.getColumnName());
    }

    /**
     * For a  given Column in a plainSelect, find the Table object from TAP Schema
     * 
     * @param tapSchema
     * @param plainSelect
     * @param column
     * @return
     */
    private static Table getQualifiedTable(TapSchema tapSchema, PlainSelect plainSelect, Column column)
    {
        // Possible form as:
        // columnName, table.columnName, tableAilas.columnName, or schema.table.ColumnName
        // must not be a selectItem alias.

        Table qTable = null;

        Table table = column.getTable();
        if (table == null || table.getName() == null || table.getName().equals(""))
        {
            // columnName only
            qTable = findTableForColumnName(tapSchema, plainSelect, column.getColumnName());
        }
        else
        {
            // table.columnName, tableAilas.columnName, or schema.table.ColumnName

            if (table.getSchemaName() != null && !table.getSchemaName().equals(""))
            {
                qTable = table;
            }
            else
            {
                String tableNameOrAlias = table.getName();
                qTable = ParserUtil.findFromTable(plainSelect, tableNameOrAlias);
            }
        }
        return qTable;
    }

    /**
     * Find the FunctionDesc for a given Function.
     *
     * @param tapSchema
     * @param function
     * @return FunctionDesc
     */
    public static FunctionDesc findFunctionDesc(TapSchema tapSchema, Function function)
    {
        if (function == null || function.getName() == null)
            return null;

        for (FunctionDesc functionDesc : tapSchema.getFunctionDescs())
        {
            log.debug("check: " + function.getName() + " vs " + functionDesc.getName());
            if (function.getName().equalsIgnoreCase(functionDesc.getName()))
                return functionDesc;
        }
        return null;
    }
    
}
