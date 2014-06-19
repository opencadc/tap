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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.tap.writer.format.DefaultFormatFactory;
import ca.nrc.cadc.tap.writer.format.FormatFactory;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * Class for setting up the parsing of query parameters (LANG and associated 
 * parameters), instantiating, and executing a suitable TapQUery implementation.
 * 
 * </p><p>
 * This class supports extension by delegating calls to a subclass of itself 
 * named <code>ca..nrc.cadc.tap.impl.QueryProcessorImpl</code>. Delegate implementation
 * is optional. The purpose of creating  a delegate is to override the default binding 
 * of LANG values to TapQuery implementation classes via the getLangBindings() method.
 * 
 * @author pdowler
 */
public class PluginFactory 
{
    private static final Logger log = Logger.getLogger(PluginFactory.class);
    
    private static final String CONFIG = PluginFactory.class.getSimpleName() + ".properties";
    
    private Job job;
    private Properties config;
    private Map<String, Class> langBindings = new HashMap<String,Class>();
    
    public PluginFactory(Job job) 
    { 
        this.job = job;
        init();
    }
    
    @Override
    public String toString()
    {
        return this.getClass().getName() + "[" + config.entrySet().size() + "]";
    }
    private void init()
    {
        // default config
        Class c = AdqlQuery.class;
        langBindings.put("ADQL", c);
        langBindings.put("ADQL-2.0", c);
        
        this.config = new Properties();
        URL url = null;
        try
        {
            
            url = PluginFactory.class.getClassLoader().getResource(CONFIG);
            if (url != null)
                config.load(url.openStream());
        }
        catch(Exception ex) 
        { 
            throw new RuntimeException("failed to read " + CONFIG + " from " + url, ex);
        }
        
        // configured LANG bindings
        String langStr = config.getProperty(TapQuery.class.getName() + ".langValues");
        if (langStr != null)
        {
            String langs[] = langStr.split(" ");
            for (String lang : langs)
            {
                String cname = config.getProperty(lang);
                if (cname != null)
                {
                    try
                    {
                        c = Class.forName(cname);
                        langBindings.put(lang, c);
                    }
                    catch(ClassNotFoundException ex)
                    {
                        log.error("CONFIG: failed to load " + cname + " for LANG="+lang, ex);
                    }
                }
            }
        }
    }
    
    public TapQuery getTapQuery()
    {
        String lang = ParameterUtil.findParameterValue("LANG", job.getParameterList());
        if (lang == null || lang.length() == 0)
            throw new IllegalArgumentException("missing required parameter: LANG");

        Class clazz = langBindings.get(lang);
        if (clazz == null)
            throw new IllegalArgumentException("unknown LANG: " + lang);
        try
        {
            TapQuery ret = (TapQuery) clazz.newInstance();
            log.debug("created: " + ret.getClass().getName());
            ret.setJob(job);
            return ret;
        }
        catch(Exception ex)
        {
            throw new RuntimeException("config error: failed to create " + clazz.getName(), ex);
        }
    }

    public MaxRecValidator getMaxRecValidator()
    {
        MaxRecValidator ret = null;
        String name = MaxRecValidator.class.getName();
        String cname = config.getProperty(name);
        if (cname == null)
            ret = new MaxRecValidator();
        else
        {
            try
            {
                Class c = Class.forName(cname);
                ret = (MaxRecValidator) c.newInstance();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("config error: failed to create MaxRecValidator " + cname, ex);
            }
        }
        ret.setJob(job);
        return  ret;
    }
    
    public UploadManager getUploadManager()
    {
        UploadManager ret = null;
        String name = UploadManager.class.getName();
        String cname = config.getProperty(name);
        if (cname == null)
            ret = new DefaultUploadManager();
        else
        {
            try
            {
                Class c = Class.forName(cname);
                ret = (UploadManager) c.newInstance();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("config error: failed to create UploadManager " + cname, ex);
            }
        }
        ret.setJob(job);
        return  ret;
    }
    
    public TableWriter getTableWriter()
    {
        TableWriter ret = null;
        String name = TableWriter.class.getName();
        String cname = config.getProperty(name);
        if (cname == null)
            ret = new DefaultTableWriter();
        else
        {
            try
            {
                Class c = Class.forName(cname);
                ret = (TableWriter) c.newInstance();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("config error: failed to create TableWriter " + cname, ex);
            }
        }
        ret.setJob(job);
        return  ret;
    }
    
    public FormatFactory getFormatFactory()
    {
        FormatFactory ret = null;
        String name = FormatFactory.class.getName();
        String cname = config.getProperty(name);
        if (cname == null)
            ret = new DefaultFormatFactory();
        else
        {
            try
            {
                Class c = Class.forName(cname);
                ret = (FormatFactory) c.newInstance();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("config error: failed to create FormatFactory " + cname, ex);
            }
        }
        ret.setJob(job);
        return  ret;
    }
    
    public TapSchemaDAO getTapSchemaDAO()
    {
        TapSchemaDAO ret = null;
        String name = TapSchemaDAO.class.getName();
        String cname = config.getProperty(name);
        if (cname == null)
            ret = new TapSchemaDAO();
        else
        {
            try
            {
                Class c = Class.forName(cname);
                ret = (TapSchemaDAO) c.newInstance();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("config error: failed to create TapSchemaDAO " + cname, ex);
            }
        }
        ret.setJob(job);
        return  ret;
    }
    
    public ResultStore getResultStore()
    {
        String name = ResultStore.class.getName();
        String cname = config.getProperty(name);
        try
        {
            Class c = Class.forName(cname);
            ResultStore ret = (ResultStore) c.newInstance();
            ret.setJob(job);
            log.debug("loaded: " + ret.getClass().getName());
            return  ret;
        }
        catch(Exception ex)
        {
            throw new RuntimeException("config error: failed to create ResultStore " + cname, ex);
        }
    }
}
