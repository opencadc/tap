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
************************************************************************
*/

package org.opencadc.youcat;


import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.vosi.actions.DataSourceProvider;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class DataSourceProviderImpl extends DataSourceProvider {
    private static final Logger log = Logger.getLogger(DataSourceProviderImpl.class);

    public DataSourceProviderImpl() { 
    }

    @Override
    public DataSource getDataSource(String requestPath) {
        String db = extractDatabaseFromPath(requestPath);
        String dsname = getDataSourceName(db, "tapadm");
        try {
            log.debug("JDNI lookup: " + dsname);
            return DBUtil.findJNDIDataSource(dsname);
        } catch (NamingException ex) {
            throw new RuntimeException("CONFIG: failed to find datasource " + dsname, ex);
        }
    }
    
    static String getDataSourceNameForJob(Job job, String connectionType) {
        String path = job.getRequestPath();
        String dbName = extractDatabaseFromPath(path);
        String dsName = getDataSourceName(dbName, connectionType);
        log.debug("request path: " + path + "database: " + dbName + " datasource: " + dsName);
        return dsName;
    }
    
    // used for availability checks
    static String getDataSourceName(String db, String connectionType) {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc/");
        if (db != null) {
            sb.append(db).append("-");
        }
        sb.append(connectionType);
        return sb.toString();
    }
    
    static String extractDatabaseFromPath(String requestPath) {
        // single monolithic database
        // 0: empty str
        // 1: service name
        // 2: resource name (sync|async|tables)
        return null;
        
        //String[] ss = requestPath.split("/");
        // 0: empty str
        // 1: service name
        // 2: database name
        //return ss[2];
        // 3: resource name (sync|async|tables)
    }
}
