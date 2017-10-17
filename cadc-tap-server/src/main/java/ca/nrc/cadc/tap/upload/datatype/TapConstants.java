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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.tap.upload.datatype;

import ca.nrc.cadc.tap.upload.VOTableParserException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Constants from various TAP specifications.
 */
public class TapConstants
{
    public static final String TAP10_SMALLINT = "adql:SMALLINT";
    public static final String TAP10_INTEGER = "adql:INTEGER";
    public static final String TAP10_BIGINT = "adql:BIGINT";
    public static final String TAP10_REAL = "adql:REAL";
    public static final String TAP10_DOUBLE = "adql:DOUBLE";
    public static final String TAP10_CHAR = "adql:CHAR";
    public static final String TAP10_VARCHAR = "adql:VARCHAR";
    public static final String TAP10_TIMESTAMP = "adql:TIMESTAMP";
    public static final String TAP10_CLOB = "adql:CLOB";
    public static final String TAP10_POINT = "adql:POINT";
    public static final String TAP10_REGION = "adql:REGION";
    
    //public static final String VOT_CHAR = "char";
    //public static final String VOT_SHORT = "short";
    //public static final String VOT_INT = "int";
    //public static final String VOT_LONG = "long";
    //public static final String VOT_FLOAT = "float";
    //public static final String VOT_DOUBLE = "double";
    //public static final String VOT_BOOLEAN = "boolean";
    //public static final String VOT_BYTE = "byte";
    //public static final String VOT_UNSIGNED_BYTE = "unsignedByte";

    //public static final String XTYPE_TIMESTAMP = "timestamp";
    //public static final String XTYPE_INTERVAL = "interval";
    //public static final String XTYPE_POINT = "point";
    //public static final String XTYPE_CIRCLE = "circle";
    //public static final String XTYPE_POLYGON = "polygon";
}
