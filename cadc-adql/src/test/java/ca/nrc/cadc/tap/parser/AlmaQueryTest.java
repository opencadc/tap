package ca.nrc.cadc.tap.parser;

import ca.nrc.cadc.tap.schema.*;
import ca.nrc.cadc.util.Log4jInit;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.fail;

/**
 * Created by jburke on 2017-08-29.
 */
public class AlmaQueryTest
{
    private static Logger log = Logger.getLogger(AlmaQueryTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap", org.apache.log4j.Level.DEBUG);
    }

    String alamQuery = "SELECT\n" +
            " distinct asa_ous.asa_project_code AS \"Project code\",\n" +
            "asa_science.source_name AS \"Source name\",\n" +
            "asa_science.ra AS \"RA\",\n" +
            "asa_science.dec AS \"Dec\",\n" +
            "asa_science.gal_longitude AS \"Galactic longitude\",\n" +
            "asa_science.gal_latitude AS \"Galactic latitude\",\n" +
            "asa_science.band_list AS \"Band\",\n" +
            "to_number(asa_science.spatial_resolution) AS \"Spatial resolution\",\n" +
            "asa_science.frequency_resolution AS \"Frequency resolution\",\n" +
            "concat(concat(case when asa_science.ant_main_num > 0 then '12m' else '' end, case when asa_science.ant_aca_num > 0 then '7m' else '' end), case when asa_science.ant_tp_num > 0 then 'TP' else '' end) as \"Array\",\n" +
            "(case when asa_science.is_mosaic = 'Y' then 'mosaic' else '' end) as \"Mosaic\",\n" +
            "asa_science.int_time AS \"Integration\",\n" +
            "to_char(asa_delivery_status.release_date, 'YYYY-MM-DD') AS \"Release date\",\n" +
            "asa_science.frequency_support AS \"Frequency support\",\n" +
            "asa_science.velocity_resolution AS \"Velocity resolution\",\n" +
            "asa_science.pol_products AS \"Pol products\",\n" +
            "to_char(asa_science.start_date, 'YYYY-MM-DD HH24:MI:SS') AS \"Observation date\",\n" +
            "asa_project.pi_name AS \"PI name\",\n" +
            "asa_science.schedblock_name as \"SB name\",\n" +
            "asa_project.coi_name AS \"Proposal authors\",\n" +
            "min(asa_energy.sensitivity_10kms) AS \"Line sensitivity (10 km/s)\",\n" +
            "asa_science.cont_sensitivity_bandwidth AS \"Continuum sensitivity\",\n" +
            "asa_science.pwv AS \"PWV\",\n" +
            "asa_ous.group_ous_uid as \"Group ous id\",\n" +
            "asa_ous.member_ous_uid as \"Member ous id\",\n" +
            "asa_science.asdm_uid AS \"Asdm uid\",\n" +
            "asa_project.title AS \"Project title\",\n" +
            "asa_project.type AS \"Project type\",\n" +
            "asa_science.scan_intent AS \"Scan intent\",\n" +
            "asa_science.fov AS \"Field of view\",\n" +
            "asa_science.spatial_scale_max AS \"Largest angular scale\",\n" +
            "asa_delivery_status.qa2_passed AS \"QA2 Status\",\n" +
            "(select count(distinct asab.bibcode) from asa_project_bibliography asab where project_code = asa_ous.asa_project_code) AS \"Pub\",\n" +
            "asa_project.science_keyword AS \"Science keyword\",\n" +
            "asa_project.scientific_category AS \"Scientific category\",\n" +
            "asa_science.footprint AS \"Footprint\"\n" +
            " FROM ALMA_JBURKE.asa_science\n" +
            "  inner join ALMA_JBURKE.asa_science rawdata on rawdata.parent_dataset_id = asa_science.dataset_id\n" +
            "  inner join ALMA_JBURKE.aqua_execblock on rawdata.asdm_uid = aqua_execblock.execblockuid\n" +
            "  inner join ALMA_JBURKE.asa_ous on asa_science.asa_ous_id = asa_ous.asa_ous_id\n" +
            "  inner join ALMA_JBURKE.asa_project on rawdata.project_code = asa_project.code\n" +
            "  left outer join ALMA_JBURKE.asa_project_bibliography on rawdata.project_code = asa_project_bibliography.project_code\n" +
            "  left outer join ALMA_JBURKE.asa_delivery_asdm_ous on rawdata.asdm_uid = asa_delivery_asdm_ous.asdm_uid\n" +
            "  left outer join ALMA_JBURKE.asa_delivery_status on asa_delivery_asdm_ous.deliverable_name = asa_delivery_status.delivery_id\n" +
            "  inner join ALMA_JBURKE.asa_energy on asa_science.dataset_id = asa_energy.asa_dataset_id\n" +
            "WHERE (1=1)\n" +
            " AND asa_science.product_type = 'MOUS'\n" +
            "  AND (asa_delivery_status.qa2_passed is null or asa_delivery_status.qa2_passed = 'Y')\n" +
            " AND lower(asa_ous.asa_project_code) not like '%.csv'\n" +
            " AND  ( asa_science.DEC BETWEEN 3.503224444444445 AND 43.50322444444444\n" +
            " AND  asa_science.RA BETWEEN 211.9078227716832 AND 255.5690438949835\n" +
            " AND (asin(sqrt((-0.542402 - asa_science.cx) * (-0.542402 - asa_science.cx) + (-0.739431 - asa_science.cy) * (-0.739431 - asa_science.cy) + (0.398801 - asa_science.cz) * (0.398801 - asa_science.cz)) / 2.0) < 0.174533))\n" +
            " AND  (lower(ASA_SCIENCE.scan_intent) LIKE '%target%')\n" +
            "GROUP BY asa_ous.asa_project_code, asa_science.source_name, asa_science.ra, asa_science.dec, asa_science.gal_longitude, asa_science.gal_latitude, asa_science.band_list, asa_science.spatial_resolution, asa_science.frequency_resolution, asa_science.ant_main_num, asa_science.ant_aca_num, asa_science.ant_tp_num, asa_science.is_mosaic, asa_science.int_time, asa_delivery_status.release_date, asa_science.frequency_support, asa_science.velocity_resolution, asa_science.pol_products, asa_science.start_date, asa_project.pi_name, asa_science.schedblock_name, asa_project.coi_name, asa_science.cont_sensitivity_bandwidth, asa_science.pwv, asa_ous.group_ous_uid, asa_ous.member_ous_uid, asa_science.asdm_uid, asa_project.title, asa_project.type, asa_science.scan_intent, asa_science.fov, asa_science.spatial_scale_max, asa_delivery_status.qa2_passed, asa_project.science_keyword, asa_project.scientific_category, asa_science.footprint\n" +
            " ORDER BY \"Release date\" asc";

    String countDistinctQuery = "select count (distinct asab.bibcode) from asa_project_bibliography asab where project_code = asa_ous.asa_project_code";

    String query3 = "select count (distinct foo.bar) from example foo";

    @Test
    public void testAlmaQuery()
    {
        doTest(alamQuery);
    }

    @Test
    public void testCountDistinct()
    {
        doTest(countDistinctQuery);
    }

    private void doTest(final String query)
    {
        log.debug("query:\n" + query);
        try
        {
            StringReader sr = new StringReader(query);
            CCJSqlParserManager sqlParser = new CCJSqlParserManager();
            Statement statement = sqlParser.parse(sr);
        }
        catch (Exception e)
        {
            fail("test failure because " + e.getLocalizedMessage());
            log.error(e);
        }
    }

}
