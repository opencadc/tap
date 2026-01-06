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
************************************************************************
*/

package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.tap.DefaultTableWriter;
import ca.nrc.cadc.tap.schema.InitDatabaseTS;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.uws.server.impl.InitDatabaseUWS;
import ca.nrc.cadc.vosi.actions.DeleteAction;
import ca.nrc.cadc.vosi.actions.TablesAction;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class YoucatInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(YoucatInitAction.class);

    private static final String YOUCAT = YoucatInitAction.class.getPackageName();
    private static final String YOUCAT_ADMIN = YOUCAT + ".adminUser";
    private static final String YOUCAT_CREATE = YOUCAT + ".createSchemaInDB";
    private static final String DEFAULT_VOTABLE_SERIALIZATION_KEY = YOUCAT + ".defaultVOTableSerialization";
    private static final String DELETED_SCHEMA_KEY = YOUCAT + ".deletedSchemaName";

    private String jndiAdminKey;
    private String jndiCreateSchemaKey;
    
    private Boolean createSchemaInDB = false;
    private String defaultVOTableSerialization = null;
    private String deletedSchemaName = null;
    
    public YoucatInitAction() { 
    }

    private void initConfig() {
        this.jndiAdminKey = appName + TablesAction.ADMIN_KEY;
        this.jndiCreateSchemaKey = appName + TablesAction.CREATE_SCHEMA_KEY;
        
        PropertiesReader r = new PropertiesReader("youcat.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        boolean ok = true;

        Subject admin = null;
        String adminUserStr = mvp.getFirstPropertyValue(YOUCAT_ADMIN);
        sb.append("\n\t").append(YOUCAT_ADMIN).append(": ");
        if (adminUserStr == null) {
            sb.append("MISSING");
        } else {
            try {
                IdentityManager im = AuthenticationUtil.getIdentityManager();
                admin = im.toSubject(adminUserStr);
                String str = admin.toString().replaceAll("\n", " ");
                sb.append(str).append(" OK");
            } catch (Exception ex) {
                ok = false;
                sb.append(ex.toString());
                sb.append(" ERROR");
            }
        }
        
        String yc = mvp.getFirstPropertyValue(YOUCAT_CREATE);
        sb.append("\n\t").append(YOUCAT_CREATE).append(": ");
        if (yc == null) {
            sb.append("false (default)");
        } else {
            if ("true".equals(yc)) {
                createSchemaInDB = true;
                sb.append(createSchemaInDB).append(" OK");
            } else if ("false".equals(yc)) {
                createSchemaInDB = false;
                sb.append(createSchemaInDB).append(" OK");
            } else {
                sb.append(yc).append(" INVALID boolean");
                ok = false;
            }
        }

        String defaultVOTableSerializationValue = mvp.getFirstPropertyValue(DEFAULT_VOTABLE_SERIALIZATION_KEY);
        sb.append("\n\t").append(DEFAULT_VOTABLE_SERIALIZATION_KEY).append(": ");
        if (defaultVOTableSerializationValue == null) {
            sb.append("TABLEDATA (default)");
            defaultVOTableSerialization = "TABLEDATA";
        } else {
            if (VOTableWriter.SerializationType.TABLEDATA.toString().equals(defaultVOTableSerializationValue)) {
                defaultVOTableSerialization = defaultVOTableSerializationValue;
                sb.append(defaultVOTableSerializationValue).append(" OK");
            } else if (VOTableWriter.SerializationType.BINARY2.toString().equals(defaultVOTableSerializationValue)) {
                defaultVOTableSerialization = defaultVOTableSerializationValue;
                sb.append(defaultVOTableSerializationValue).append(" OK");
            } else {
                sb.append(defaultVOTableSerializationValue).append(" INVALID VOTable serialization");
                ok = false;
            }
        }
        
        String deletedSchemaValue = mvp.getFirstPropertyValue(DELETED_SCHEMA_KEY);
        sb.append("\n\t").append(DELETED_SCHEMA_KEY).append(": ");
        if (deletedSchemaValue == null) {
            sb.append("null (default)");
        } else {
            // TODO: validate schema name here
            sb.append(deletedSchemaValue).append(" OK");
            deletedSchemaName = deletedSchemaValue;
        }
        
        log.info("init:" + sb.toString());
        
        if (!ok) {
            throw new InvalidConfigException(sb.toString());
        }
        
        try {
            Context ctx = new InitialContext();
            if (adminUserStr != null) {
                ctx.bind(jndiAdminKey, admin);
            }
            if (createSchemaInDB != null) {
                ctx.bind(jndiCreateSchemaKey, createSchemaInDB);
            }
        } catch (Exception ex) {
            log.error("Failed to create JNDI key(s): " + jndiAdminKey + "|" + jndiCreateSchemaKey, ex);
        }
    }
    
    @Override
    public void doInit() {
        try {
            initConfig();
            
            // tap_schema
            log.info("InitDatabaseTS: START");
            DataSource tapadm = DBUtil.findJNDIDataSource("jdbc/tapadm");
            InitDatabaseTS tsi = new InitDatabaseTS(tapadm, null, "tap_schema");
            tsi.doInit();
            log.info("InitDatabaseTS: OK");
            
            // uws schema
            log.info("InitDatabaseUWS: START");
            DataSource uws = DBUtil.findJNDIDataSource("jdbc/uws");
            InitDatabaseUWS uwsi = new InitDatabaseUWS(uws, null, "uws");
            uwsi.doInit();
            log.info("InitDatabaseUWS: OK");
            
            DefaultTableWriter.setDefaultVOTableSerialization(defaultVOTableSerialization);
            DeleteAction.setDeletedSchemaName(deletedSchemaName);

        } catch (Exception ex) {
            throw new RuntimeException("INIT FAIL: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void doShutdown() {
        super.doShutdown();
    }
}
