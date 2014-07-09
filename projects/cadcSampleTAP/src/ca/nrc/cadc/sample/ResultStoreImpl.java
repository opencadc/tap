/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2014.                            (c) 2014.
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

package ca.nrc.cadc.sample;

import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.tap.ResultStore;
import ca.nrc.cadc.uws.Job;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * Basic ResultStore implementation usable by the cadcTAP QueryRunner for async
 * result storage. This implementation simply writes files to a local directory
 * and then returns a URL to that file. The filesystem-to-URL mapping is configurable
 * via two properties in a config file names ResultStoreImpl.properties which is 
 * loaded from the classpath.
 * </p>
 * <ul>
 * <li>ca.nrc.cadc.tap.impl.ResultStoreImpl.baseStorageDir=/path/to/storage
 * <li>ca.nrc.cadc.tap.impl.ResultStoreImpl.baseURL=http://hostname/storage
 * </ul>
 * 
 * @author pdowler
 */
public class ResultStoreImpl implements ResultStore
{
    private static final Logger log = Logger.getLogger(ResultStoreImpl.class);
 
    private static final String CONFIG = ResultStoreImpl.class.getSimpleName() + ".properties";
    private static final String BASE_DIR_KEY = ResultStoreImpl.class.getName() + ".baseStorageDir";
    private static final String BASE_URL_KEY = ResultStoreImpl.class.getName() + ".baseURL";
    
    private Job job;
    private String contentType;
    private String filename;
    
    private String baseDir;
    private String baseURL;
    
    public ResultStoreImpl() 
    { 
        try
        {
            URL url = ResultStoreImpl.class.getClassLoader().getResource(CONFIG);
            log.debug("read: " + url.toExternalForm());
            Properties props = new Properties();
            props.load(url.openStream());
            for (String s : props.stringPropertyNames())
                log.debug("props: " + s + "=" + props.getProperty(s));
            this.baseDir = props.getProperty(BASE_DIR_KEY);
            this.baseURL = props.getProperty(BASE_URL_KEY);
        }
        catch(Exception ex)
        {
            log.error("CONFIG: failed to load/read config from ResultStoreImpl.properties", ex);
            throw new RuntimeException("CONFIG: failed to load/read config from ResultStoreImpl.properties", ex);
        }
        if (baseDir == null || baseURL == null)
        {
            log.error("CONFIG: incomplete: baseDir=" + baseDir +"  baseURL=" + baseURL);
            throw new RuntimeException("CONFIG incomplete: baseDir=" + baseDir +" baseURL=" + baseURL);
        }
    }

    public URL put(ResultSet rs, TableWriter<ResultSet> writer) 
        throws IOException
    {
        return put(rs, writer, null);
    }
    public URL put(ResultSet rs, TableWriter<ResultSet> writer, Integer maxRows) 
        throws IOException
    {
        Long num = null;
        if (maxRows != null)
            num = new Long(maxRows.intValue());
        
        File dest = getDestFile(filename);
        URL ret = getURL(filename);
        FileOutputStream ostream = null;
        try
        {
            ostream = new FileOutputStream(dest);
            writer.write(rs, ostream, num);
        }
        finally
        {
            if (ostream != null)
                ostream.close();
        }
        return ret;
    }

    public URL put(Throwable t, VOTableWriter writer) throws IOException
    {
        File dest = getDestFile(filename);
        URL ret = getURL(filename);
        FileOutputStream ostream = null;
        try
        {
            ostream = new FileOutputStream(dest);
            writer.write(t, ostream);
        }
        finally
        {
            if (ostream != null)
                ostream.close();
        }
        return ret;
    }

    public void setJob(Job job)
    {
        this.job = job;
    }

    public void setContentType(String contentType)
    {
        this.contentType = contentType;
    }

    public void setFilename(String filename)
    {
        this.filename = filename;
    }
    
    private File getDestFile(String filename)
    {
        File dir = new File(baseDir);
        if (!dir.exists())
            throw new RuntimeException(BASE_DIR_KEY + "=" + baseDir + " does not exist");
        if (!dir.isDirectory())
            throw new RuntimeException(BASE_DIR_KEY + "=" + baseDir + " is not a directory");
        if (!dir.canWrite())
            throw new RuntimeException(BASE_DIR_KEY + "=" + baseDir + " is not writable");
        
        return new File(dir, filename);
    }
    
    private URL getURL(String filename)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(baseURL);
        
        if ( !baseURL.endsWith("/") )
            sb.append("/");
        
        sb.append(filename);
        String s = sb.toString();
        try
        {
            return new URL(s);
        }
        catch(MalformedURLException ex)
        {
            throw new RuntimeException("failed to create URL from " + s, ex);
        }
    }
}
