
package org.opencadc.tap.ws;


import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.MemoryJobPersistence;
import ca.nrc.cadc.uws.server.SimpleJobManager;
import ca.nrc.cadc.uws.server.ThreadPoolExecutor;


/**
 * @author pdowler
 */
public class QueryJobManager extends SimpleJobManager {
    private static final long MAX_EXEC_DURATION = 4 * 3600L;    // 4 hours to dump a catalog to vpsace
    private static final long MAX_DESTRUCTION = 7 * 24 * 60 * 60L; // 1 week
    private static final long MAX_QUOTE = 24 * 3600L;         // 24 hours since we have a threadpool with
    // queued jobs

    public QueryJobManager() {
        super();

        // Persist UWS jobs to memory by default.
        final MemoryJobPersistence jobPersist = new MemoryJobPersistence();

        // max threads: 6 == number of simultaneously running async queries (per
        // web server), plus sync queries, plus VOSI-tables queries
        final JobExecutor jobExec = new ThreadPoolExecutor(jobPersist, QueryRunner.class, 6);

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }
}
