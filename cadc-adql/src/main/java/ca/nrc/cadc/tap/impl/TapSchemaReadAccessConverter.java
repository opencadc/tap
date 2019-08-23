/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.tap.impl;

import java.net.URI;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.gms.GroupClient;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.GroupUtil;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.server.StringIDGenerator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;;

/**
 * Query converter that injects read-access constraints for tap_schema queries.
 *
 * @author pdowler, majorb
 */
public class TapSchemaReadAccessConverter extends SelectNavigator {
    
    private static Logger log = Logger.getLogger(TapSchemaReadAccessConverter.class);
    
    private static final String schemaOwnerColumn = "owner_id";
    private static final String schemaReadAnonColumn = "read_anon";
    private static final String schemaReadOnlyGroupColumn = "read_only_group";
    private static final String schemaReadWriteGroupColumn = "read_write_group";

    public static class AssetTable {
        
        public String schema;
        public String name;
        public String wholeName;
        public String keyColumn;

        AssetTable() {
        }

        AssetTable(String schema, String name, String keyColumn) {
            this.schema = schema;
            this.name = name;
            this.wholeName = new String(schema + "." + name).toLowerCase();
            this.keyColumn = keyColumn;
        }
    }

    public static final AssetTable SCHEMAS_ASSET_TABLE = new AssetTable("tap_schema", "schemas", "schema_name");
    public static final AssetTable TABLES_ASSET_TABLE = new AssetTable("tap_schema", "tables", "table_name");
    public static final AssetTable COLUMNS_ASSET_TABLE = new AssetTable("tap_schema", "columns", "column_name");

    private GroupClient gmsClient;
    private IdentityManager identityManager;

    public TapSchemaReadAccessConverter(IdentityManager identityManager) {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
        this.identityManager = identityManager;
    }

    // testing support
    public void setGroupClient(GroupClient gmsClient) {
        this.gmsClient = gmsClient;
    }

    @Override
    public void visit(PlainSelect ps) {
        log.debug("start - visit(PlainSelect) " + ps);
        super.visit(ps);
        Expression exprAccessControl = accessControlExpression(ps);
        if (exprAccessControl == null) {
            return; 
        }

        Expression where = ps.getWhere();
        if (where == null)
            ps.setWhere(exprAccessControl);
        else {
            Parenthesis left = new Parenthesis(where);
            Parenthesis right = new Parenthesis(exprAccessControl);
            Expression and = new AndExpression(left, right);
            ps.setWhere(and);
        }
        log.debug("end - visit(PlainSelect) " + ps);
    }

    private Expression accessControlExpression(PlainSelect ps) {
        List<Expression> exprAcList = new ArrayList<Expression>();
        List<Table> fromTableList = ParserUtil.getFromTableList(ps);
        for (Table assetTable : fromTableList) {
            String fromTableWholeName = assetTable.getWholeTableName().toLowerCase();
            log.debug("check: " + fromTableWholeName);

            if (SCHEMAS_ASSET_TABLE.wholeName.equals(fromTableWholeName)) {
                exprAcList.add(schemasAccessControlExpression(assetTable));
            } else if (TABLES_ASSET_TABLE.wholeName.equals(fromTableWholeName)) {
                exprAcList.add(tablesAccessControlExpression(assetTable));
            } else if (COLUMNS_ASSET_TABLE.wholeName.equals(fromTableWholeName)) {
                exprAcList.add(columnsAccessControlExpression(assetTable));
            } else {
                log.debug("not an asset table: " + fromTableWholeName);
            }
        }
        // AC list : one expression per asset table
        if (exprAcList.size() > 0) {
            return combineAndExpressions(exprAcList);
        }
        return null;
    }
    
    private Expression schemasAccessControlExpression(Table schemasAssetTable) {
        
        // WHERE:
        // 
        // key_col is null OR
        // <schemasAccessControlClause>
        
        Column schemasKeyCol = new Column(schemasAssetTable, SCHEMAS_ASSET_TABLE.keyColumn);
        Expression publicByNullKey = publicByKeyColumn(schemasKeyCol);
        Expression accessControlClause = accessControlWhereClause(schemasAssetTable);    
        return new OrExpression(publicByNullKey, accessControlClause);
    }
    
    private Expression tablesAccessControlExpression(Table tablesAssetTable) {
        
        // WHERE:
        // 
        // schema_name in
        //   (select tap_schema.schemas.schema_name from tap_schema.schemas
        //    where <schemasAccessControlExpresssion>);
        
        PlainSelect ps = new PlainSelect();
        
        // create the select item list for the subselect 
        SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
        Table subSelectSchemasTable = new Table(SCHEMAS_ASSET_TABLE.schema, SCHEMAS_ASSET_TABLE.name);
        
        // create the from expression for the subselect
        Column subSelectSelectCol = new Column(subSelectSchemasTable, SCHEMAS_ASSET_TABLE.keyColumn);
        selectExpressionItem.setExpression(subSelectSelectCol);
        ps.setSelectItems(Arrays.asList(selectExpressionItem));
        
        ps.setFromItem(subSelectSchemasTable);
        
        // create the subselect where clause
        Expression schemasAccessControlExpression = accessControlWhereClause(subSelectSchemasTable);
        ps.setWhere(schemasAccessControlExpression);
        
        // create the subselect
        SubSelect itemsList = new SubSelect();
        itemsList.setSelectBody(ps);
        
        // put together the in expression
        InExpression inExpression = new InExpression();
        Column tablesForeignKeyCol = new Column(tablesAssetTable, SCHEMAS_ASSET_TABLE.keyColumn);
        inExpression.setLeftExpression(useTableAliasIfExists(tablesForeignKeyCol));
        inExpression.setItemsList(itemsList);
        
        Column tablesKeyCol = new Column(tablesAssetTable, TABLES_ASSET_TABLE.keyColumn);
        Expression publicByNull = this.publicByKeyColumn(tablesKeyCol);
        
        return new OrExpression(publicByNull, inExpression);
    }
    
    private Expression columnsAccessControlExpression(Table columnsAssetTable) {
        
        // WHERE:
        //
        // table_name in
        //   (select table_name from tap_schema.tables t
        //    join tap_schema.schemas s on t.schema_name=s.schema_name
        //    where <schemasAccessControlExpression>);
        
        PlainSelect ps = new PlainSelect();
        
        // create the select item list for the subselect 
        SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
        Table tablesAssetTable = new Table(TABLES_ASSET_TABLE.schema, TABLES_ASSET_TABLE.name);
        StringIDGenerator idGenerator = new RandomStringGenerator(8);
        tablesAssetTable.setAlias(idGenerator.getID());
        
        // create the from expression for the subselect
        Column tablesKeyCol = new Column(tablesAssetTable, TABLES_ASSET_TABLE.keyColumn);
        selectExpressionItem.setExpression(useTableAliasIfExists(tablesKeyCol));
        ps.setSelectItems(Arrays.asList(selectExpressionItem));
        
        ps.setFromItem(tablesAssetTable);
        
        // add a join to schemas
        Join joinToSchemas = new Join();
        
        Table schemasAssetTable = new Table(SCHEMAS_ASSET_TABLE.schema, SCHEMAS_ASSET_TABLE.name);
        schemasAssetTable.setAlias(idGenerator.getID());
        
        joinToSchemas.setRightItem(schemasAssetTable);
        EqualsTo onExpression = new EqualsTo();

        Column tablesOnColumn = new Column(tablesAssetTable, SCHEMAS_ASSET_TABLE.keyColumn);
        onExpression.setLeftExpression(useTableAliasIfExists(tablesOnColumn));
        
        Column schemasOnCol = new Column(schemasAssetTable, SCHEMAS_ASSET_TABLE.keyColumn);
        onExpression.setRightExpression(useTableAliasIfExists(schemasOnCol));
        joinToSchemas.setOnExpression(onExpression);
        ps.setJoins(Arrays.asList(joinToSchemas));
        
        // create the subselect where clause
        Expression schemasAccessControlExpression = accessControlWhereClause(schemasAssetTable);
        ps.setWhere(schemasAccessControlExpression);
        
        // create the subselect
        SubSelect itemsList = new SubSelect();
        itemsList.setSelectBody(ps);
        
        // put together the in expression
        InExpression inExpression = new InExpression();
        
        Column leftExpression = new Column(columnsAssetTable, TABLES_ASSET_TABLE.keyColumn);
        inExpression.setLeftExpression(useTableAliasIfExists(leftExpression));
        inExpression.setItemsList(itemsList);
        
        Column columnsKeyCol = new Column(columnsAssetTable, COLUMNS_ASSET_TABLE.keyColumn);
        Expression publicByNull = this.publicByKeyColumn(columnsKeyCol);
        return new OrExpression(publicByNull, inExpression);
    }
    
    private Expression accessControlWhereClause(Table schemasAssetTable) {
        
        //   WHERE:
        // 
        //   owner is null OR
        //   (owner_id is not null AND read_anon=1) OR
        //   (owner_id = <ownerID>) OR
        //   (read_only_group IN (<group1>, <group2>, ... <groupN>)) OR
        //   (read_write_group IN (<group1>, <group2>, ... <groupN>))

        Expression accessControlExpr = null;
        Expression publicByNullOwner = publicByNullOwner(schemasAssetTable);
        Expression publicByPublicTrue = publicByPublicTrue(schemasAssetTable);
        
        Expression pub = new OrExpression(publicByNullOwner,
                publicByPublicTrue);
                    
        if (isAuthenticated()) {
        
            Expression authorizedByOwner = authorizedByOwner(schemasAssetTable);
            
            List<String> gids = null;
            log.debug("gmsClient: " + gmsClient);
            if (gmsClient != null) {
                gids = getGroupIDs(gmsClient);
            }
            
            if (gids != null && gids.size() > 0) {
                Expression authorizedByReadGroup = authorizedByReadGroup(schemasAssetTable, gids);
                Expression authorizedByReadWriteGroup =  authorizedByReadWriteGroup(schemasAssetTable, gids);
                accessControlExpr = new OrExpression(authorizedByOwner,
                    new OrExpression(authorizedByReadGroup,
                        authorizedByReadWriteGroup));
            } else {
                accessControlExpr = authorizedByOwner;
            }
            
        }

        if (accessControlExpr != null) {
            accessControlExpr = new OrExpression(pub, accessControlExpr);
        } else {
            accessControlExpr = pub;
        }
        return accessControlExpr;
    }

    // if keyColumn is null, this is a join that didn't match any rows
    private Expression publicByKeyColumn(Column keyColumn) {
        Column columnMeta = useTableAliasIfExists(keyColumn);
        IsNullExpression isNull = new IsNullExpression();
        isNull.setLeftExpression(columnMeta);
        return isNull;
    }
    
    private Expression publicByNullOwner(Table schemaTable) {
        Column columnMeta = useTableAliasIfExists(new Column(schemaTable, schemaOwnerColumn));
        IsNullExpression isNull = new IsNullExpression();
        isNull.setLeftExpression(columnMeta);
        return isNull;
    }
    
    private Expression publicByPublicTrue(Table schemaTable) {
        Column ownerColumnMeta = useTableAliasIfExists(new Column(schemaTable, schemaOwnerColumn));
        IsNullExpression isNull = new IsNullExpression();
        isNull.setNot(true);
        isNull.setLeftExpression(ownerColumnMeta);
        
        Column publicColumnMeta = useTableAliasIfExists(new Column(schemaTable, schemaReadAnonColumn));
        EqualsTo equals = new EqualsTo();
        equals.setLeftExpression(publicColumnMeta);
        equals.setRightExpression(new LongValue("1"));
        
        return new Parenthesis(new AndExpression(isNull, equals));
    }
    
    private Expression authorizedByOwner(Table schemaTable) {
        Column columnMeta = useTableAliasIfExists(new Column(schemaTable, schemaOwnerColumn));
        EqualsTo equals = new EqualsTo();
        equals.setLeftExpression(columnMeta);
        String owner = identityManager.toOwnerString(AuthenticationUtil.getCurrentSubject());
        equals.setRightExpression(new StringValue("'" + owner + "'"));
        return equals;
    }
    
    private Expression authorizedByReadGroup(Table schemaTable, List<String> groupIDs) {
        Column columnMeta = useTableAliasIfExists(new Column(schemaTable, schemaReadOnlyGroupColumn));
        InExpression in = new InExpression();
        in.setLeftExpression(columnMeta);
        in.setItemsList(createStringExpressionList(groupIDs));
        return in;
    }
    
    private Expression authorizedByReadWriteGroup(Table schemaTable, List<String> groupIDs) {
        Column columnMeta = useTableAliasIfExists(new Column(schemaTable, schemaReadWriteGroupColumn));
        InExpression in = new InExpression();
        in.setLeftExpression(columnMeta);
        in.setItemsList(createStringExpressionList(groupIDs));
        return in;
    }
    
    private static ExpressionList createStringExpressionList(List<String> list) {
        List<Expression> expressions = new ArrayList<>(list.size());
        StringValue stringValue = null;
        for (String next : list) {
            stringValue = new StringValue("'" + next + "'");
            expressions.add(stringValue);
        }
        ExpressionList expressionList = new ExpressionList();
        expressionList.setExpressions(expressions);
        return expressionList;
    }

    private static Expression combineAndExpressions(List<Expression> exprList) {
        Expression rtn = null;
        for (Expression expr : exprList) {
            if (rtn == null) {
                rtn = expr;
            } else {
                rtn = new AndExpression(new Parenthesis(rtn), new Parenthesis(expr));
            }
        }
        return rtn;
    }

    private static Column useTableAliasIfExists(Column column) {
        Column rtn = null;
        Table newTable = null;

        Table table = column.getTable();
        if (table == null)
            rtn = column; // no treatment is made, return original column
        else {
            String alias = table.getAlias();
            if (alias == null || alias.equals(""))
                rtn = column; // no treatment is made, return original column
            else {
                newTable = new Table(null, alias);
                rtn = new Column(newTable, column.getColumnName());
            }
        }
        return rtn;
    }
    
    private boolean isAuthenticated() {
        Subject s = AuthenticationUtil.getCurrentSubject();
        return s != null && s.getPrincipals() != null && s.getPrincipals().size() > 0;
    }

    private List<String> getGroupIDs(GroupClient gmsClient) throws AccessControlException {
        try {
            List<String> groupIDs = new ArrayList<String>();

            try {
                if (ensureCredentials()) {
                    GroupClient gms = gmsClient;
                    if (gms == null) {
                        log.debug("Constructing new GMS Client");
                        LocalAuthority loc = new LocalAuthority();
                        URI gmsURI = loc.getServiceURI(Standards.GMS_GROUPS_01.toString());
                        gms = GroupUtil.getGroupClient(gmsURI);
                    }
                    List<GroupURI> groups = gms.getMemberships();
                    for (GroupURI group : groups) {
                        groupIDs.add(group.toString());
                    }
                }
            } catch (CertificateException ex) {
                throw new RuntimeException("failed to find group memberships (invalid proxy certficate)", ex);
            }
            return groupIDs;
        } finally {
        }
    }
    
    // testing support
    boolean ensureCredentials() throws AccessControlException,
            CertificateExpiredException, CertificateNotYetValidException {
        return CredUtil.checkCredentials();
    }
        
}