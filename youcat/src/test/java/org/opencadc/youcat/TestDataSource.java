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
*  $Revision: 1 $
*
************************************************************************
*/

package org.opencadc.youcat;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class TestDataSource
{
    private static final Logger LOG = Logger.getLogger(TestDataSource.class);
    
    public static DataSource getDataSource(String server, String database)
    {
        try
        {
            DBConfig dbConfig = new DBConfig();
            ConnectionConfig connectionConfig = dbConfig.getConnectionConfig(server, database);
            DataSource ds = DBUtil.getDataSource(connectionConfig, true, true);
            return ds;
        }
        catch (Exception ex)
        {
            LOG.error(ex);
        }
        
        return null;
    }

}
