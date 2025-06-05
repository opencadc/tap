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

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.tap.schema.InitDatabaseTS;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.uws.server.impl.InitDatabaseUWS;
import ca.nrc.cadc.vosi.actions.TablesAction;
import javax.naming.Context;
import javax.naming.InitialContext;
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

    public YoucatInitAction() {
    }

    private void initConfig() {
        String jndiAdminKey = appName + TablesAction.ADMIN_KEY;
        String jndiCreateSchemaKey = appName + TablesAction.CREATE_SCHEMA_KEY;
        
        PropertiesReader r = new PropertiesReader("youcat.properties");
        MultiValuedProperties mvp = r.getAllProperties();

        StringBuilder sb = new StringBuilder();
        sb.append("incomplete config: ");
        boolean ok = true;

        String adminUser = mvp.getFirstPropertyValue(YOUCAT_ADMIN);
        sb.append("\n\t").append(YOUCAT_ADMIN).append(": ");
        if (adminUser == null) {
            sb.append("MISSING");
        } else {
            sb.append("OK");
        }
        
        String yc = mvp.getFirstPropertyValue(YOUCAT_CREATE);
        sb.append("\n\t").append(YOUCAT_CREATE).append(": ");
        if (yc == null) {
            sb.append("MISSING");
        } else {
            sb.append("OK");
        }
        
        if (!ok) {
            throw new InvalidConfigException(sb.toString());
        }
        
        boolean createSchemaInDB = "true".equals(yc); // default: false for backwards compat
        try {
            Context ctx = new InitialContext();
            if (adminUser != null) {
                ctx.bind(jndiAdminKey, new HttpPrincipal(adminUser));
            }
            ctx.bind(jndiCreateSchemaKey, createSchemaInDB);
            log.info("init: admin=" + adminUser + " createSchemaInDB=" + createSchemaInDB);
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

            // ServiceDescriptors
            log.info("InitDatabaseYoucat: START");
            InitDatabaseYoucat youcat = new InitDatabaseYoucat(tapadm, null, "tap_schema");
            youcat.doInit();
            log.info("InitDatabaseYoucat: OK");
        } catch (Exception ex) {
            throw new RuntimeException("INIT FAIL: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void doShutdown() {
        super.doShutdown();
    }
}
