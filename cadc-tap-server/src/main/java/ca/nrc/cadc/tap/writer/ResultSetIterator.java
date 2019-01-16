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

package ca.nrc.cadc.tap.writer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.writer.format.ResultSetFormat;
import java.sql.SQLWarning;
import org.apache.log4j.Logger;

public class ResultSetIterator implements Iterator<List<Object>>
{
    private static final Logger log = Logger.getLogger(ResultSetIterator.class);
    
    private ResultSet rs;
    private boolean hasNext;
    private List<Format<Object>> formats;
    private long numRows = 0l;
    private long prevRows = -1l;
    private long prevTime = -1l;

    public ResultSetIterator(ResultSet rs, List<Format<Object>> formats)
    {

        if (rs != null && formats != null)
        {
            try
            {
                if (rs.getMetaData().getColumnCount() != formats.size())
                    throw new IllegalArgumentException("Wrong number of formats for result set.");
            }
            catch (SQLException e)
            {
                throw new RuntimeException(e.getMessage());
            }
        }

        this.rs = rs;
        this.formats = formats;
        if (rs == null)
        {
            this.hasNext = false;
            return;
        }
        try
        {
            this.hasNext = rs.next();
        }
        catch(SQLException e)
        {
            hasNext = false;
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 
     * @return number of rows returned
     */
    public long getRowCount()
    {
        return numRows;
    }
    
    

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public List<Object> next()
    {
        if (prevTime < 0l)
            prevTime = System.currentTimeMillis();
        if (prevRows < 0l)
            prevRows = 0l;
        
        try
        {
            // If no more rows in the ResultSet throw a NoSuchElementException.
            if (!hasNext)
                throw new NoSuchElementException("No more rows in the ResultSet");

            // check/clear interrupted flag and throw if necessary
            if ( Thread.interrupted() )
                throw new RuntimeException(new InterruptedException());
            
            List<Object> next = new ArrayList<>();
            Object nextObj = null;
            Format<Object> nextFormat = null;

            for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++)
            {
                nextFormat = formats.get(columnIndex - 1);
                if (nextFormat instanceof ResultSetFormat)
                    nextObj = ((ResultSetFormat) nextFormat).extract(rs, columnIndex);
                else
                    nextObj = rs.getObject(columnIndex);
                next.add(nextObj);
            }
            
            // Get the next row.
            hasNext = rs.next();
            numRows++;
            SQLWarning sw = rs.getWarnings();
            while (sw != null)
            {
                log.warn("result set warning: " + sw.getMessage());
                sw = sw.getNextWarning();
            }
            rs.clearWarnings();
            
            // track rate we are able to output rows
            long t1 = System.currentTimeMillis();
            long dt = t1 - prevTime;
            if (dt >= 10000l || !hasNext) // every 10 sec
            {
                long dr = numRows - prevRows;
                double rate = 1000.0 * ((double) dr) / ((double) dt); // rows/sec
                log.debug("row output: " + dr + " rows in " + dt + "ms = " + rate + " rows/sec " + numRows);
             
                prevRows = numRows;
                prevTime = t1;
            }

            
            return next;
        }
        catch (SQLException e)
        {
            hasNext = false;
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
