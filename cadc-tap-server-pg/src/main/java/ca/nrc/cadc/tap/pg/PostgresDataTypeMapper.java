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
************************************************************************
*/

package ca.nrc.cadc.tap.pg;


import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.postgresql.PgInterval;
import ca.nrc.cadc.dali.postgresql.PgScircle;
import ca.nrc.cadc.dali.postgresql.PgSpoint;
import ca.nrc.cadc.dali.postgresql.PgSpoly;
import ca.nrc.cadc.tap.db.BasicDataTypeMapper;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.log4j.Logger;
import org.postgresql.util.PGobject;

/**
 *
 * @author pdowler
 */
public class PostgresDataTypeMapper extends BasicDataTypeMapper {
    private static final Logger log = Logger.getLogger(PostgresDataTypeMapper.class);

    public PostgresDataTypeMapper() {
        dataTypes.put(TapDataType.POINT, new TypePair("spoint", null));
        dataTypes.put(TapDataType.CIRCLE, new TypePair("scircle", null));
        dataTypes.put(TapDataType.POLYGON, new TypePair("spoly", null));
        dataTypes.put(TapDataType.INTERVAL, new TypePair("polygon", null));
        
        dataTypes.put(new TapDataType("char", "*", "uri"), new TypePair("CHAR", Types.CHAR));
        dataTypes.put(new TapDataType("char", "36", "uuid"), new TypePair("uuid", null));
        
        dataTypes.put(new TapDataType("char", "*", "adql:POINT"), new TypePair("spoint", null));
        dataTypes.put(new TapDataType("char", "*", "adql:REGION"), new TypePair("spoly", null));
    }

    @Override
    public String getIndexColumnOperator(ColumnDesc columnDesc) {
        return null;
    }

    @Override
    public String getIndexUsingQualifier(ColumnDesc columnDesc, boolean unique) {
        TypePair tp = findTypePair(columnDesc.getDatatype());
        switch(tp.str) {
            case "spoint":
            case "scircle":
            case "spoly":
            case "polygon":
                if (unique) {
                    throw new IllegalArgumentException("unique index not supported for column type: " + columnDesc.getDatatype());
                }
                return "gist";
            default:
                return null;
        }
    }
    
    @Override
    public Object getPointObject(ca.nrc.cadc.stc.Position pos)
    {
        Point p = new Point(pos.getCoordPair().getX(), pos.getCoordPair().getY());
        return getPointObject(p);
    }

    @Override
    public Object getRegionObject(ca.nrc.cadc.stc.Region reg)
    {
        if (reg instanceof ca.nrc.cadc.stc.Polygon)
        {
            ca.nrc.cadc.stc.Polygon poly = ( ca.nrc.cadc.stc.Polygon) reg;
            Polygon p = new Polygon();
            for (ca.nrc.cadc.stc.CoordPair c : poly.getCoordPairs()) {
                p.getVertices().add(new Point(c.getX(), c.getY()));
            }
            return getPolygonObject(p);
        }
        throw new UnsupportedOperationException("cannot convert a " + reg.getClass().getSimpleName());
    }
    
    @Override
    public Object getPointObject(Point p)
    {
        PgSpoint pgs = new PgSpoint();
        PGobject pgo = pgs.generatePoint(p);
        return pgo;
    }

    @Override
    public Object getCircleObject(Circle c) {
        PgScircle pgs = new PgScircle();
        PGobject pgo = pgs.generateCircle(c);
        return pgo;
    }
    
    @Override
    public Object getPolygonObject(Polygon poly)
    {
        PgSpoly pgs = new PgSpoly();
        PGobject pgo = pgs.generatePolygon(poly);
        return pgo;
    }

    @Override
    public Object getIntervalObject(DoubleInterval inter)
    {
        PgInterval gen = new PgInterval();
        return gen.generatePolygon2D(inter);
    }

    @Override
    public Object getIntervalArrayObject(DoubleInterval[] inter)
    {
        PgInterval gen = new PgInterval();
        return gen.generatePolygon2D(inter);
    }
}
