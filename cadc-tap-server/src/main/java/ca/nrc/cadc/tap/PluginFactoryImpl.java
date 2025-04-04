/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Class for setting up the parsing of query parameters (LANG and associated
 * parameters), instantiating, and executing a suitable TapQuery implementation.
 *
 * @author pdowler
 */
public class PluginFactoryImpl extends PluginFactory {
    private static final Logger log = Logger.getLogger(PluginFactoryImpl.class);

    private Job job;
    
    private Map<String, Class> langBindings = new HashMap<>();

    public PluginFactoryImpl(Job job) {
        super();
        this.job = job;
        init();
    }


    @Override
    public String toString() {
        return this.getClass().getName() + "[" + config.entrySet().size() + "]";
    }

    private void init() {
        // default config
        
        // configured LANG bindings
        Class c;
        String langStr = config.getProperty(TapQuery.class.getName() + ".langValues");
        if (langStr != null) {
            String[] langs = langStr.split(" ");
            for (String lang : langs) {
                String cname = config.getProperty(lang);
                if (cname != null) {
                    try {
                        c = Class.forName(cname);
                        langBindings.put(lang, c);
                    } catch (Throwable ex) {
                        log.error("CONFIG: failed to loadFormatFactory " + cname + " for LANG=" + lang, ex);
                    }
                }
            }
        }
    }

    /**
     * This value determines if an auto-commit (true) or manual (false) transaction is used to
     * execute the main query. The default value is false because it seems like JDBC drivers 
     * need that to use a cursor to control fetching batches of rows and auto-commit silently
     * disabled that behaviour. The preferred value can be configured with:
     * <code>ca.nrc.cadc.tap.QueryRunner.autoCommit=true|false</code>.
     * 
     * @return 
     */
    public boolean getAutoCommit() {
        String name = QueryRunner.class.getName() + ".autoCommit";
        String cval = config.getProperty(name);
        if (cval == null) {
            return false;
        }
        try {
            return Boolean.getBoolean(cval);
        } catch (Exception ex) {
            throw new RuntimeException("CONFIG: invalid value for " + name + " in PluginFactory.properties: " + cval);
        }
    }
    
    public TapQuery getTapQuery() {
        String lang = ParameterUtil.findParameterValue("LANG", job.getParameterList());
        if (lang == null || lang.length() == 0) {
            throw new IllegalArgumentException("missing required parameter: LANG");
        }

        Class clazz = langBindings.get(lang);
        if (clazz == null) {
            throw new IllegalArgumentException("unknown LANG: " + lang);
        }
        try {
            TapQuery ret = (TapQuery) clazz.newInstance();
            log.debug("created: " + ret.getClass().getName());
            ret.setJob(job);
            return ret;
        } catch (Throwable ex) {
            throw new RuntimeException("config error: failed to create " + clazz.getName(), ex);
        }
    }

    public MaxRecValidator getMaxRecValidator() {
        final MaxRecValidator ret;
        String name = MaxRecValidator.class.getName();
        String cname = config.getProperty(name);
        if (cname == null) {
            ret = new MaxRecValidator();
        } else {
            try {
                Class c = Class.forName(cname);
                ret = (MaxRecValidator) c.newInstance();
            } catch (Throwable ex) {
                throw new RuntimeException("config error: failed to create MaxRecValidator " + cname, ex);
            }
        }
        ret.setJob(job);
        return ret;
    }

    public UploadManager getUploadManager() {
        final UploadManager ret;
        String name = UploadManager.class.getName();
        String cname = config.getProperty(name);
        if (cname == null) {
            ret = new DefaultUploadManager();
        } else {
            try {
                Class c = Class.forName(cname);
                ret = (UploadManager) c.newInstance();
            } catch (Throwable ex) {
                throw new RuntimeException("config error: failed to create UploadManager " + cname, ex);
            }
        }
        ret.setJob(job);
        return ret;
    }

    public TableWriter getTableWriter() {
        final TableWriter ret;
        String name = TableWriter.class.getName();
        String cname = config.getProperty(name);
        if (cname == null) {
            ret = new DefaultTableWriter();
        } else {
            try {
                Class c = Class.forName(cname);
                ret = (TableWriter) c.newInstance();
            } catch (Throwable ex) {
                throw new RuntimeException("config error: failed to create TableWriter " + cname, ex);
            }
        }
        ret.setJob(job);
        ret.setFormatFactory(getFormatFactory());
        return ret;
    }
    
    // this makes DefaultTableWriter lenient about requested format
    // so that writing error document never fails
    public TableWriter getErrorWriter() {
        final TableWriter ret;
        String name = TableWriter.class.getName();
        String cname = config.getProperty(name);
        if (cname == null) {
            DefaultTableWriter dtw = new DefaultTableWriter(true);
            ret = dtw;
        } else {
            try {
                Class c = Class.forName(cname);
                ret = (TableWriter) c.newInstance();
            } catch (Throwable ex) {
                throw new RuntimeException("config error: failed to create TableWriter " + cname, ex);
            }
        }
        ret.setJob(job);
        ret.setFormatFactory(getFormatFactory());
        return ret;
    }
    
    public FormatFactory getFormatFactory() {
        FormatFactory ret;
        String name = FormatFactory.class.getName();
        String cname = config.getProperty(name);
        if (cname == null) {
            ret = new DefaultFormatFactory();
        } else {
            try {
                Class c = Class.forName(cname);
                ret = (FormatFactory) c.newInstance();
            } catch (Throwable ex) {
                throw new RuntimeException("config error: failed to create FormatFactory " + cname, ex);

            }
        }

        ret.setJob(job);

        return ret;
    }

    public ResultStore getResultStore() {
        String name = ResultStore.class.getName();
        String cname = config.getProperty(name);
        try {
            Class c = Class.forName(cname);
            ResultStore ret = (ResultStore) c.newInstance();
            ret.setJob(job);
            log.debug("loaded: " + ret.getClass().getName());
            return ret;
        } catch (Throwable ex) {
            throw new RuntimeException("config error: failed to create ResultStore " + cname, ex);
        }
    }

    @Override
    public TapSchemaDAO getTapSchemaDAO() {
        TapSchemaDAO ret = super.getTapSchemaDAO();
        ret.setJob(job);
        return ret;
    }
}
