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

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ColumnIndex;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import org.apache.log4j.Logger;

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator.VisitingPart;
import ca.nrc.cadc.tap.schema.TapSchema;

/**
 * Validate Column and ColumnIndex.
 * 
 * @author zhangsa
 *
 */
public class TapSchemaColumnValidator extends ReferenceNavigator
{
    protected static Logger log = Logger.getLogger(TapSchemaColumnValidator.class);

    protected TapSchema tapSchema;

    public TapSchemaColumnValidator()
    {
    }

    public TapSchemaColumnValidator(TapSchema ts)
    {
        this.tapSchema = ts;
    }

    public void setTapSchema(TapSchema tapSchema)
    {
        this.tapSchema = tapSchema;
    }

    /* (non-Javadoc)
     * @see net.sf.jsqlparser.statement.select.ColumnReferenceVisitor#visit(net.sf.jsqlparser.statement.select.ColumnIndex)
     */
    @Override
    public void visit(ColumnIndex columnIndex)
    {
        log.debug("visit(columnIndex)" + columnIndex);
        // TODO this is non-tapschema validation, move to someplace else?
        int ci = columnIndex.getIndex();
        if (ci > ParserUtil.countSelectItems(selectNavigator.getPlainSelect()))
            throw new IllegalArgumentException("ColumnIndex " + columnIndex + " is out of scope.");
    }

    /* (non-Javadoc)
     * @see net.sf.jsqlparser.statement.select.ColumnReferenceVisitor#visit(net.sf.jsqlparser.schema.Column)
     */
    @Override
    public void visit(Column column)
    {
        log.debug("visit(column)" + column);
        // The column may be referred by alias, by columnName, by table.columnName, tableAilas.columnName, or by schema.table.ColumnName

        PlainSelect plainSelect = selectNavigator.getPlainSelect();
        log.debug("plainSelect is:" + plainSelect);
        VisitingPart visiting = selectNavigator.getVisitingPart();
        log.debug("visiting is:" + visiting);
        if (visiting.equals(VisitingPart.SELECT_ITEM) || visiting.equals(VisitingPart.FROM)
                //|| visiting.equals(VisitingPart.GROUP_BY)
                )
        {
            // cannot be by alias
            // possible forms: columnName, table.columnName, tableAilas.columnName, or schema.table.ColumnName
            TapSchemaUtil.validateColumnNonAlias(tapSchema, plainSelect, column);
        }
        else
        // visiting WHERE, HAVING, ORDER BY
        {
            // can be by alias
            // Possible form as:
            // alias, columnName, table.columnName, tableAilas.columnName, or schema.table.ColumnName
            boolean isAlias = false;
            Table table = column.getTable();
            if (table == null || table.getName() == null || table.getName().equals(""))
            {
                // form: alias, or columnName
                String columnNameOrAlias = column.getColumnName();
                SelectItem selectItem = ParserUtil.findSelectItemByAlias(plainSelect, columnNameOrAlias);
                if (selectItem != null) // it's an alias, found selectItem
                    isAlias = true; // ok
            }

            if (!isAlias) TapSchemaUtil.validateColumnNonAlias(tapSchema, plainSelect, column);
        }
    }
}
