/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2016.                            (c) 2016.
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
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap;

import ca.nrc.cadc.tap.schema.*;

import ca.nrc.cadc.uws.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;


public class SqlQueryTest
{
    @Test
    public void withHaving() throws Exception
    {
        final String query = "SELECT DISTINCT TABLE1.COL1A AS \"COL1A\"," +
                             " TABLE1.COL1B AS \"COL1B\"," +
                             " TABLE2.COL2A AS \"COL2A\"," +
                             " TABLE3.COL3A AS \"COL3A\"" +
                             " FROM SCHEMA1.TABLE1" +
                             "  inner join SCHEMA1.TABLE2 on TABLE1.COL1A = TABLE2.COL2A" +
                             "  inner join SCHEMA1.TABLE3 on TABLE2.COL2B = TABLE3.COL3A" +
                             " WHERE (1=1)" +
                             " AND TABLE3.COL3A = 'VAL'" +
                             " GROUP BY TABLE1.COL1A, TABLE1.COL1B, TABLE2.COL2A, TABLE3.COL3A" +
                             " HAVING TABLE2.COL2A = (" +
                             "SELECT MAX(T3.COL3A) AS MM FROM TABLE3 AS T3 " +
                             "inner join TABLE1 as t1 on t1.COL1A = T3.COL3A " +
                             "inner join TABLE2 as t2 on t2.COL2B = t1.COL1B " +
                             "WHERE t1.COL1A = TABLE1.COL1A" +
                             ")" +
                             " ORDER BY \"COL1A\" asc";

        final String expected = "SELECT DISTINCT TABLE1.COL1A AS \"COL1A\"," +
                             " TABLE1.COL1B AS \"COL1B\"," +
                             " TABLE2.COL2A AS \"COL2A\"," +
                             " TABLE3.COL3A AS \"COL3A\"" +
                             " FROM SCHEMA1.TABLE1" +
                             " INNER JOIN SCHEMA1.TABLE2 ON TABLE1.COL1A = TABLE2.COL2A" +
                             " INNER JOIN SCHEMA1.TABLE3 ON TABLE2.COL2B = TABLE3.COL3A" +
                             " WHERE (1 = 1)" +
                             " AND TABLE3.COL3A = 'VAL'" +
                             " GROUP BY TABLE1.COL1A, TABLE1.COL1B, TABLE2.COL2A, TABLE3.COL3A" +
                             " HAVING TABLE2.COL2A = (" +
                             "SELECT MAX(T3.COL3A) AS MM FROM TABLE3 AS T3 " +
                             "INNER JOIN TABLE1 AS t1 ON t1.COL1A = T3.COL3A " +
                             "INNER JOIN TABLE2 AS t2 ON t2.COL2B = t1.COL1B " +
                             "WHERE t1.COL1A = TABLE1.COL1A" +
                             ")" +
                             " ORDER BY \"COL1A\" ASC";

        final SqlQuery testSubject = new SqlQuery();
        final List<FunctionDesc> functionDescs = new ArrayList<>();
        functionDescs.add(new FunctionDesc("MAX", null, "int"));

        final List<SchemaDesc> schemaDescs = new ArrayList<>();
        final SchemaDesc schemaDesc = new SchemaDesc("SCHEMA1", "TEST SCHEMA",
                                                     "UTYPE.1");
        final List<TableDesc> tableDescs = new ArrayList<>();

        final List<ColumnDesc> columnDescs1 = new ArrayList<>();
        final List<ColumnDesc> columnDescs2 = new ArrayList<>();
        final List<ColumnDesc> columnDescs3 = new ArrayList<>();

        columnDescs1.add(new ColumnDesc("TABLE1", "COL1A", "TEST COL1A",
                                        "UTYPE.TABLE1.COL1A", "", "", "", 88));
        columnDescs1.add(new ColumnDesc("TABLE1", "COL1B", "TEST COL1B",
                                        "UTYPE.TABLE1.COL1B", "", "", "", 88));

        columnDescs2.add(new ColumnDesc("TABLE2", "COL2A", "TEST COL2A",
                                        "UTYPE.TABLE2.COL2A", "", "", "", 88));
        columnDescs2.add(new ColumnDesc("TABLE2", "COL2B", "TEST COL2B",
                                        "UTYPE.TABLE2.COL2B", "", "", "", 88));

        columnDescs3.add(new ColumnDesc("TABLE3", "COL3A", "TEST COL3A",
                                        "UTYPE.TABLE3.COL3A", "", "", "", 88));

        final TableDesc tableDesc1 = new TableDesc("SCHEMA1", "TABLE1",
                                                   "TEST TABLE1",
                                                   "UTYPE.TABLE.1");
        tableDesc1.setColumnDescs(columnDescs1);

        final TableDesc tableDesc2 = new TableDesc("SCHEMA1", "TABLE2",
                                                   "TEST TABLE2",
                                                   "UTYPE.TABLE.2");
        tableDesc2.setColumnDescs(columnDescs2);

        final TableDesc tableDesc3 = new TableDesc("SCHEMA1", "TABLE3",
                                                   "TEST TABLE3",
                                                   "UTYPE.TABLE.3");
        tableDesc3.setColumnDescs(columnDescs3);

        tableDescs.add(tableDesc1);
        tableDescs.add(tableDesc2);
        tableDescs.add(tableDesc3);
        schemaDesc.setTableDescs(tableDescs);
        schemaDescs.add(schemaDesc);

        final TapSchema tapSchema = new TapSchema(schemaDescs);
        tapSchema.setFunctionDescs(functionDescs);

        final Calendar currentTime = Calendar.getInstance();
        final Calendar destructTime = Calendar.getInstance();
        destructTime.set(Calendar.HOUR, destructTime.get(Calendar.HOUR) + 1);

        final List<Result> results = new ArrayList<>();
        final List<Parameter> params = new ArrayList<>();
        params.add(new Parameter("QUERY", query));
        params.add(new Parameter("LANG", "SQL"));

        final JobInfo jobInfo = new JobInfo("", "", true);

        final Job job = new Job(ExecutionPhase.EXECUTING, 88L,
                                destructTime.getTime(), null,
                                currentTime.getTime(), null,
                                currentTime.getTime(), null, "testuser", null,
                                "/run", "", jobInfo, params, results);

        testSubject.setTapSchema(tapSchema);
        testSubject.setJob(job);

        final String sql = testSubject.getSQL();

        assertEquals("Query returned does not match.", expected, sql);
    }
}
