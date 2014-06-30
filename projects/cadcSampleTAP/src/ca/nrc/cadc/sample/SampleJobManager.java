
package ca.nrc.cadc.sample;

import ca.nrc.cadc.tap.QueryRunner;
import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.MemoryJobPersistence;
import ca.nrc.cadc.uws.server.SimpleJobManager;
import ca.nrc.cadc.uws.server.ThreadPoolExecutor;
import org.apache.log4j.Logger;

/**
 * This is a sample JobManager for use in a TAP service. Both the async and sync
 * queries can use this class to manage jobs. The component choices made below
 * setup the simplest possible configuration. The class name for this class is
 * used in the web.xml to configure the async and sync UWS resources.
 *
 * @author pdowler
 */
public class SampleJobManager extends SimpleJobManager
{
    private static Logger log = Logger.getLogger(SampleJobManager.class);

    /**
     * Sample job manager implementation. This class extends the SimpleJobManager
     * and sets up the persistence and executor classes in the constructor. It uses
     * the MemoryJobPersistence implementation and the ThreadExecutor implementation
     * and thus can handle both sync and async jobs.
     */
    public SampleJobManager()
    {
        super();
        MemoryJobPersistence jobPersist = new MemoryJobPersistence();
        log.debug("created: " + jobPersist.getClass().getName());
        
        // this implementation spawns a new thread for every async job
        //JobExecutor jobExec = new ThreadExecutor(jobPersist, QueryRunner.class);

        // this implementation uses a thread pool for async jobs (with 2 threads)
        JobExecutor jobExec = new ThreadPoolExecutor(jobPersist, QueryRunner.class, 2);
        log.debug("created: " + jobExec.getClass().getName());
        
        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        
        // these are the default values from super class SimpleJobManager
        //setMaxExecDuration(3600L);     // one hour
        //setMaxQuote(3600L);            // one hour 
        //setMaxDestruction(7*24*3600L); // 7 days
    }
}
