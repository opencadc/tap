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

import ca.nrc.cadc.vosi.AvailabilityStatus;
import ca.nrc.cadc.vosi.WebService;
import ca.nrc.cadc.vosi.avail.CheckDataSource;
import ca.nrc.cadc.vosi.avail.CheckException;
import org.apache.log4j.Logger;


/**
 * Sample WebService implementation for VOSI-availability. The class name for this class
 * is used to configure the VOSI-availability servlet in the web.xml file.
 * 
 * @author pdowler
 */
public class SampleWebService implements WebService
{
    private static final Logger log = Logger.getLogger(SampleWebService.class);
    
    private static String TAPDS_NAME = "jdbc/tapuser";
    // note tap_schema table names
    private String TAPDS_TEST = "select schema_name from tap_schema.schemas11 where schema_name='tap_schema'";
    
    public SampleWebService()
    {
        
    }
    
    public AvailabilityStatus getStatus()
    {
        boolean isGood = true;
        String note = "service is accepting queries";
        try
        {
            // test query using standard TAP data source
            CheckDataSource checkDataSource = new CheckDataSource(TAPDS_NAME, TAPDS_TEST);
            checkDataSource.check();
            
            // check for a certficate needed to perform network ops
            //File cert = ...
            //CheckCertificate checkCert = new CheckCertificate(cert);
            //checkCert.check();

            // check some other web service availability since we depend it
            //URL avail = ...
            //CheckWebService cws = new CheckWebService(avail);
            //cws.check();
        }
        catch(CheckException ce)
        {
            // tests determined that the resource is not working
            isGood = false;
            note = ce.getMessage();
        }
        catch (Throwable t)
        {
            // the test itself failed
            log.error("web service status test failed", t);
            isGood = false;
            note = "test failed, reason: " + t;
        }
        return new AvailabilityStatus(isGood, null, null, null, note);
    }

    public void setState(String string)
    {
        throw new UnsupportedOperationException();
    }
    
}
