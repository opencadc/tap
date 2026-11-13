/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.upload;

import ca.nrc.cadc.dali.tables.ListTableData;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.tap.BasicUploadManager;
import ca.nrc.cadc.tap.db.OracleTableLoader;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.upload.UploadLimits;
import java.io.IOException;
import java.util.List;
import org.opencadc.tap.io.TableDataInputStream;

/**
 * Oracle UPLOAD: binds DALI {@code point} / {@code polygon} as {@code SDO_GEOMETRY} via
 * {@link OracleTableLoader}. Use this instead of {@link BasicUploadManager} on Oracle deployments.
 */
public class OracleUploadManager extends BasicUploadManager {

    public OracleUploadManager(UploadLimits uploadLimits) {
        super(uploadLimits);
    }

    @Override
    protected void storeTable(TableDesc table, VOTableTable vot) throws IOException {
        TableCreator tc = new TableCreator(dataSource);
        tc.createTable(table);

        OracleTableLoader tld = new OracleTableLoader(dataSource, 1000);
        tld.load(table, new TableDataInputStream() {
            @Override
            public void close() {
                // fully read already
            }

            @Override
            public ResourceIterator<List<Object>> iterator() throws IOException {
                TableData tdata = vot.getTableData();
                if (tdata == null) {
                    tdata = new ListTableData();
                }
                return tdata.iterator();
            }

            @Override
            public TableDesc acceptTargetTableDesc(TableDesc td) {
                return td;
            }
        });
    }
}
