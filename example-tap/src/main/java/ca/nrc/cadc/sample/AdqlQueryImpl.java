/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.converter.TopConverter;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import org.apache.log4j.Logger;

/**
 * TAP service implementors must implement this class and add customisations of the 
 * navigatorlist as shown below. Custom query visitors can be used to validate or modify
 * the query; the base class runs all the visitors in the navigatorlist once before 
 * converting the result into SQL for execution.
 *
 * @author pdowler
 */
public class AdqlQueryImpl extends AdqlQuery
{
    private static Logger log = Logger.getLogger(AdqlQueryImpl.class);

    public AdqlQueryImpl()
    {
        super();
    }

    @Override
    protected void init()
    {
        super.init();

        // example: for postgresql we have to convert TOP to LIMIT
        super.navigatorList.add(new TopConverter(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator()));

        // TAP-1.1 tap_schema version is encoded in table names
        TableNameConverter tnc = new TableNameConverter(true);
        tnc.put("tap_schema.schemas", "tap_schema.schemas11");
        tnc.put("tap_schema.tables", "tap_schema.tables11");
        tnc.put("tap_schema.columns", "tap_schema.columns11");
        tnc.put("tap_schema.keys", "tap_schema.keys11");
        tnc.put("tap_schema.key_columns", "tap_schema.key_columns11");
        TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);
        super.navigatorList.add(new SelectNavigator(new ExpressionNavigator(), tnrc, tnc));
        
        // TODO: add more custom query visitors here
    }
}
