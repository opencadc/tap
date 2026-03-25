/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
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

package ca.nrc.cadc.tap.schema.validator.ucd;

import ca.nrc.cadc.tap.schema.validator.ucd.UCDWord.SyntaxFlag;

/**
 * Enum of all IVOA UCD1+ controlled vocabulary words as of v1.6 (December 2024).
 *
 * <p>Source: <a href="https://www.ivoa.net/Documents/UCD1+/20241218/ucd-list.txt">ucd-list</a>
 *
 * <p>Each constant encodes:
 * - the official word string
 * - the syntax flag (P, Q, S, E, C, V)
 * - description
 */
public enum UCDWordEntry {

    // arith
    arith("arith", SyntaxFlag.Q, "Arithmetic quantities"),
    arith_diff("arith.diff", SyntaxFlag.S, "Difference between two quantities described by the same UCD"),
    arith_factor("arith.factor", SyntaxFlag.P, "Numerical factor"),
    arith_grad("arith.grad", SyntaxFlag.P, "Gradient"),
    arith_rate("arith.rate", SyntaxFlag.P, "Rate (per time unit)"),
    arith_ratio("arith.ratio", SyntaxFlag.S, "Ratio between two quantities described by the same UCD"),
    arith_squared("arith.squared", SyntaxFlag.S, "Squared quantity"),
    arith_sum("arith.sum", SyntaxFlag.S, "Summed or integrated quantity"),
    arith_variation("arith.variation", SyntaxFlag.S, "Generic variation of a quantity"),
    arith_zp("arith.zp", SyntaxFlag.Q, "Zero point"),

    // em
    em("em", SyntaxFlag.S, "Electromagnetic spectrum"),
    em_IR("em.IR", SyntaxFlag.S, "Infrared part of the spectrum"),
    em_IR_J("em.IR.J", SyntaxFlag.S, "Infrared between 1.0 and 1.5 micron"),
    em_IR_H("em.IR.H", SyntaxFlag.S, "Infrared between 1.5 and 2 micron"),
    em_IR_K("em.IR.K", SyntaxFlag.S, "Infrared between 2 and 3 micron"),
    em_IR_3_4um("em.IR.3-4um", SyntaxFlag.S, "Infrared between 3 and 4 micron"),
    em_IR_4_8um("em.IR.4-8um", SyntaxFlag.S, "Infrared between 4 and 8 micron"),
    em_IR_8_15um("em.IR.8-15um", SyntaxFlag.S, "Infrared between 8 and 15 micron"),
    em_IR_15_30um("em.IR.15-30um", SyntaxFlag.S, "Infrared between 15 and 30 micron"),
    em_IR_30_60um("em.IR.30-60um", SyntaxFlag.S, "Infrared between 30 and 60 micron"),
    em_IR_60_100um("em.IR.60-100um", SyntaxFlag.S, "Infrared between 60 and 100 micron"),
    em_IR_NIR("em.IR.NIR", SyntaxFlag.S, "Near-Infrared, 1-5 microns"),
    em_IR_MIR("em.IR.MIR", SyntaxFlag.S, "Medium-Infrared, 5-30 microns"),
    em_IR_FIR("em.IR.FIR", SyntaxFlag.S, "Far-Infrared, 30-100 microns"),
    em_UV("em.UV", SyntaxFlag.S, "Ultraviolet part of the spectrum"),
    em_UV_10_50nm("em.UV.10-50nm", SyntaxFlag.S, "Ultraviolet between 10 and 50 nm (EUV extreme UV)"),
    em_UV_50_100nm("em.UV.50-100nm", SyntaxFlag.S, "Ultraviolet between 50 and 100 nm"),
    em_UV_100_200nm("em.UV.100-200nm", SyntaxFlag.S, "Ultraviolet between 100 and 200 nm (FUV Far UV)"),
    em_UV_200_300nm("em.UV.200-300nm", SyntaxFlag.S, "Ultraviolet between 200 and 300 nm (NUV near UV)"),
    em_X_ray("em.X-ray", SyntaxFlag.S, "X-ray part of the spectrum"),
    em_X_ray_soft("em.X-ray.soft", SyntaxFlag.S, "Soft X-ray (0.12 - 2 keV)"),
    em_X_ray_medium("em.X-ray.medium", SyntaxFlag.S, "Medium X-ray (2 - 12 keV)"),
    em_X_ray_hard("em.X-ray.hard", SyntaxFlag.S, "Hard X-ray (12 - 120 keV)"),
    em_bin("em.bin", SyntaxFlag.Q, "Channel / instrumental spectral bin coordinate (bin number)"),
    em_energy("em.energy", SyntaxFlag.Q, "Energy value in the em frame"),
    em_freq("em.freq", SyntaxFlag.Q, "Frequency value in the em frame"),
    em_freq_cutoff("em.freq.cutoff", SyntaxFlag.Q, "Cutoff frequency"),
    em_freq_resonance("em.freq.resonance", SyntaxFlag.Q, "Resonance frequency"),
    em_gamma("em.gamma", SyntaxFlag.S, "Gamma rays part of the spectrum"),
    em_gamma_soft("em.gamma.soft", SyntaxFlag.S, "Soft gamma ray (120 - 500 keV)"),
    em_gamma_hard("em.gamma.hard", SyntaxFlag.S, "Hard gamma ray (>500 keV)"),
    em_line("em.line", SyntaxFlag.S, "Designation of major atomic lines"),
    em_line_HI("em.line.HI", SyntaxFlag.S, "21cm hydrogen line"),
    em_line_Lyalpha("em.line.Lyalpha", SyntaxFlag.S, "H-Lyman-alpha line"),
    em_line_Lybeta("em.line.Lybeta", SyntaxFlag.S, "H-Lyman-beta line"),
    em_line_Halpha("em.line.Halpha", SyntaxFlag.S, "H-alpha line"),
    em_line_Hbeta("em.line.Hbeta", SyntaxFlag.S, "H-beta line"),
    em_line_Hgamma("em.line.Hgamma", SyntaxFlag.S, "H-gamma line"),
    em_line_Hdelta("em.line.Hdelta", SyntaxFlag.S, "H-delta line"),
    em_line_Brgamma("em.line.Brgamma", SyntaxFlag.S, "Bracket-gamma line"),
    em_line_CO("em.line.CO", SyntaxFlag.S, "CO line"),
    em_line_OIII("em.line.OIII", SyntaxFlag.S, "[OIII] line"),
    em_mm("em.mm", SyntaxFlag.S, "Millimetric part of the spectrum"),
    em_mm_30_50GHz("em.mm.30-50GHz", SyntaxFlag.S, "Millimetric between 30 and 50 GHz"),
    em_mm_50_100GHz("em.mm.50-100GHz", SyntaxFlag.S, "Millimetric between 50 and 100 GHz"),
    em_mm_100_200GHz("em.mm.100-200GHz", SyntaxFlag.S, "Millimetric between 100 and 200 GHz"),
    em_mm_200_400GHz("em.mm.200-400GHz", SyntaxFlag.S, "Millimetric between 200 and 400 GHz"),
    em_mm_400_750GHz("em.mm.400-750GHz", SyntaxFlag.S, "Millimetric between 400 and 750 GHz"),
    em_mm_750_1500GHz("em.mm.750-1500GHz", SyntaxFlag.S, "Millimetric between 750 and 1500 GHz"),
    em_mm_1500_3000GHz("em.mm.1500-3000GHz", SyntaxFlag.S, "Millimetric between 1500 and 3000 GHz"),
    em_opt("em.opt", SyntaxFlag.S, "Optical part of the spectrum"),
    em_opt_U("em.opt.U", SyntaxFlag.S, "Optical U band between 300 and 400 nm"),
    em_opt_B("em.opt.B", SyntaxFlag.S, "Optical B band between 400 and 500 nm"),
    em_opt_V("em.opt.V", SyntaxFlag.S, "Optical V band between 500 and 700 nm"),
    em_opt_R("em.opt.R", SyntaxFlag.S, "Optical R band between 550 and 900 nm"),
    em_opt_I("em.opt.I", SyntaxFlag.S, "Optical I band between 700 and 1100 nm"),
    em_pw("em.pw", SyntaxFlag.S, "Plasma waves(Trapped in local medium)"),
    em_radio("em.radio", SyntaxFlag.S, "Radio part of the spectrum"),
    em_radio_20MHz("em.radio.20MHz", SyntaxFlag.S, "Radio below 20 MHz"),
    em_radio_20_100MHz("em.radio.20-100MHz", SyntaxFlag.S, "Radio between 20 and 100 MHz"),
    em_radio_100_200MHz("em.radio.100-200MHz", SyntaxFlag.S, "Radio between 100 and 200 MHz"),
    em_radio_200_400MHz("em.radio.200-400MHz", SyntaxFlag.S, "Radio between 200 and 400 MHz"),
    em_radio_400_750MHz("em.radio.400-750MHz", SyntaxFlag.S, "Radio between 400 and 750 MHz"),
    em_radio_750_1500MHz("em.radio.750-1500MHz", SyntaxFlag.S, "Radio between 750 and 1500 MHz"),
    em_radio_1500_3000MHz("em.radio.1500-3000MHz", SyntaxFlag.S, "Radio between 1500 and 3000 MHz"),
    em_radio_3_6GHz("em.radio.3-6GHz", SyntaxFlag.S, "Radio between 3 and 6 GHz"),
    em_radio_6_12GHz("em.radio.6-12GHz", SyntaxFlag.S, "Radio between 6 and 12 GHz"),
    em_radio_12_30GHz("em.radio.12-30GHz", SyntaxFlag.S, "Radio between 12 and 30 GHz"),
    em_wavenumber("em.wavenumber", SyntaxFlag.Q, "Wavenumber value in the em frame"),
    em_wl("em.wl", SyntaxFlag.Q, "Wavelength value in the em frame"),
    em_wl_central("em.wl.central", SyntaxFlag.Q, "Central wavelength"),
    em_wl_effective("em.wl.effective", SyntaxFlag.Q, "Effective wavelength"),

    // instr
    instr("instr", SyntaxFlag.S, "Instrument"),
    instr_background("instr.background", SyntaxFlag.E, "instrumental Background"),
    instr_bandpass("instr.bandpass", SyntaxFlag.Q, "Bandpass(e.g. : band name) of instrument"),
    instr_bandwidth("instr.bandwidth", SyntaxFlag.Q, "Bandwidth of instrument"),
    instr_baseline("instr.baseline", SyntaxFlag.Q, "Baseline of interferometry"),
    instr_beam("instr.beam", SyntaxFlag.S, "Beam"),
    instr_calib("instr.calib", SyntaxFlag.Q, "Calibration parameter"),
    instr_det("instr.det", SyntaxFlag.S, "Detector"),
    instr_det_noise("instr.det.noise", SyntaxFlag.Q, "Instrument noise"),
    instr_det_psf("instr.det.psf", SyntaxFlag.Q, "Point spread function"),
    instr_det_qe("instr.det.qe", SyntaxFlag.Q, "Quantum efficiency"),
    instr_dispersion("instr.dispersion", SyntaxFlag.Q, "Dispersion of a spectrograph"),
    instr_filter("instr.filter", SyntaxFlag.S, "Filter"),
    instr_fov("instr.fov", SyntaxFlag.Q, "Related to the Field of view"),
    instr_obsty("instr.obsty", SyntaxFlag.S, "Observatory, satellite, mission"),
    instr_obsty_seeing("instr.obsty.seeing", SyntaxFlag.Q, "Seeing"),
    instr_offset("instr.offset", SyntaxFlag.Q, "Offset angle with respect to main direction of observation"),
    instr_order("instr.order", SyntaxFlag.Q, "Spectral order in a spectrograph"),
    instr_param("instr.param", SyntaxFlag.Q, "Various instrumental parameters"),
    instr_pixel("instr.pixel", SyntaxFlag.S, "Pixel(default size : angular)"),
    instr_plate("instr.plate", SyntaxFlag.S, "Photographic plate"),
    instr_plate_emulsion("instr.plate.emulsion", SyntaxFlag.Q, "Plate emulsion"),
    instr_precision("instr.precision", SyntaxFlag.Q, "Instrument precision"),
    instr_rmsf("instr.rmsf", SyntaxFlag.Q, "Rotation Measure Spread Function"),
    instr_saturation("instr.saturation", SyntaxFlag.Q, "Instrument saturation threshold"),
    instr_scale("instr.scale", SyntaxFlag.Q, "Instrument scale (for CCD, plate, image)"),
    instr_sensitivity("instr.sensitivity", SyntaxFlag.Q, "Instrument sensitivity, detection threshold"),
    instr_setup("instr.setup", SyntaxFlag.Q, "Instrument configuration or setup"),
    instr_skyLevel("instr.skyLevel", SyntaxFlag.Q, "Sky level"),
    instr_skyTemp("instr.skyTemp", SyntaxFlag.Q, "Sky temperature"),
    instr_tel("instr.tel", SyntaxFlag.S, "Telescope"),
    instr_tel_focalLength("instr.tel.focalLength", SyntaxFlag.Q, "Telescope focal length"),
    instr_voxel("instr.voxel", SyntaxFlag.S, "Related to a voxel (n-D volume element with n>2)"),

    // meta
    meta("meta", SyntaxFlag.P, "Metadata"),
    meta_abstract("meta.abstract", SyntaxFlag.P, "Abstract (of paper, proposal, etc.)"),
    meta_bib("meta.bib", SyntaxFlag.P, "Bibliographic reference"),
    meta_bib_author("meta.bib.author", SyntaxFlag.P, "Author name"),
    meta_bib_bibcode("meta.bib.bibcode", SyntaxFlag.P, "Bibcode"),
    meta_bib_fig("meta.bib.fig", SyntaxFlag.P, "Figure in a paper"),
    meta_bib_journal("meta.bib.journal", SyntaxFlag.P, "Journal name"),
    meta_bib_page("meta.bib.page", SyntaxFlag.P, "Page number"),
    meta_bib_volume("meta.bib.volume", SyntaxFlag.P, "Volume number"),
    meta_calLevel("meta.calibLevel", SyntaxFlag.Q, "Processing/calibration level"),
    meta_checksum("meta.checksum", SyntaxFlag.Q, "Numerical signature of digital data"),
    meta_code("meta.code", SyntaxFlag.P, "Code or flag"),
    meta_code_class("meta.code.class", SyntaxFlag.P, "Classification code"),
    meta_code_error("meta.code.error", SyntaxFlag.P, "Limit uncertainty error flag"),
    meta_code_member("meta.code.member", SyntaxFlag.P, "Membership code"),
    meta_code_mime("meta.code.mime", SyntaxFlag.P, "MIME type"),
    meta_code_multip("meta.code.multip", SyntaxFlag.P, "Multiplicity or binarity flag"),
    meta_code_qual("meta.code.qual", SyntaxFlag.P, "Quality, precision, reliability flag or code"),
    meta_code_status("meta.code.status", SyntaxFlag.P, "Status code (e.g.: status of a proposal/observation)"),
    meta_coverage("meta.coverage", SyntaxFlag.Q,
            "A coverage (spatial, temporal, spectral, etc) with any combination of physical axes (such as moc, tmoc, stmoc)"),
    meta_cryptic("meta.cryptic", SyntaxFlag.P, "Unknown or impossible to understand quantity"),
    meta_curation("meta.curation", SyntaxFlag.Q, "Parameter informing about the curation of the data"),
    meta_dataset("meta.dataset", SyntaxFlag.Q, "Dataset"),
    meta_email("meta.email", SyntaxFlag.Q, "Contact e-mail"),
    meta_file("meta.file", SyntaxFlag.S, "File"),
    meta_fits("meta.fits", SyntaxFlag.S, "FITS standard"),
    meta_id("meta.id", SyntaxFlag.P, "Identifier, name or designation"),
    meta_id_assoc("meta.id.assoc", SyntaxFlag.P, "Identifier of associated counterpart"),
    meta_id_CoI("meta.id.CoI", SyntaxFlag.P, "Name of Co-Investigator"),
    meta_id_cros("meta.id.cross", SyntaxFlag.P, "Cross identification"),
    meta_id_parent("meta.id.parent", SyntaxFlag.P, "Identification of parent source"),
    meta_id_part("meta.id.part", SyntaxFlag.P, "Part of identifier, suffix or sub-component"),
    meta_id_PI("meta.id.PI", SyntaxFlag.P, "Name of Principal Investigator or Co-PI"),
    meta_main("meta.main", SyntaxFlag.S, "Main value of something"),
    meta_modelled("meta.modelled", SyntaxFlag.S, "Quantity was produced by a model"),
    meta_note("meta.note", SyntaxFlag.P, "Note or remark (longer than a code or flag)"),
    meta_number("meta.number", SyntaxFlag.P, "Number (of things; e.g. nb of object in an image)"),
    meta_preview("meta.preview", SyntaxFlag.S, "Related to a preview operation for a dataset"),
    meta_query("meta.query", SyntaxFlag.Q, "A query posed to an information system or database or a property of it"),
    meta_record("meta.record", SyntaxFlag.P, "Record number"),
    meta_ref("meta.ref", SyntaxFlag.P, "Reference or origin"),
    meta_ref_doi("meta.ref.doi", SyntaxFlag.P, "DOI identifier (dereferenceable)"),
    meta_ref_epic("meta.ref.epic", SyntaxFlag.P, "ePIC identifier (dereferenceable)"),
    meta_ref_ivoid("meta.ref.ivoid", SyntaxFlag.Q, "Identifier as recommended  in the IVOA  (dereferenceable)"),
    meta_ref_orcid("meta.ref.orcid", SyntaxFlag.P, "ORCID identifier for people working in research (dereferenceable)"),
    meta_ref_pid("meta.ref.pid", SyntaxFlag.P, "Generic Persistent Identifier (dereferenceable)"),
    meta_ref_rorid("meta.ref.rorid", SyntaxFlag.P, "ROR unique and persistent identifier for research organizations"),
    meta_ref_uri("meta.ref.uri", SyntaxFlag.P, "URI, universal resource identifier"),
    meta_ref_url("meta.ref.url", SyntaxFlag.P, "URL, web address"),
    meta_software("meta.software", SyntaxFlag.S, "Software used in generating data"),
    meta_table("meta.table", SyntaxFlag.S, "Table or catalogue"),
    meta_title("meta.title", SyntaxFlag.P, "Title or explanation"),
    meta_ucd("meta.ucd", SyntaxFlag.Q, "UCD"),
    meta_unit("meta.unit", SyntaxFlag.P, "Unit"),
    meta_version("meta.version", SyntaxFlag.P, "Version"),

    // obs
    obs("obs", SyntaxFlag.S, "Observation"),
    obs_airMass("obs.airMass", SyntaxFlag.Q, "Airmass"),
    obs_atmos("obs.atmos", SyntaxFlag.S, "Atmosphere, atmospheric phenomena affecting an observation"),
    obs_atmos_extinction("obs.atmos.extinction", SyntaxFlag.Q, "Atmospheric extinction"),
    obs_atmos_humidity("obs.atmos.humidity", SyntaxFlag.Q, "Amount of air humidity at observing site during an observation"),
    obs_atmos_rain("obs.atmos.rain", SyntaxFlag.Q, "Amount of rain at the telescope site during an observation"),
    obs_atmos_refractAngle("obs.atmos.refractAngle", SyntaxFlag.Q, "Atmospheric refraction angle"),
    obs_atmos_turbulence("obs.atmos.turbulence", SyntaxFlag.S, "Related to atmospheric turbulence at the telescope site"),
    obs_atmos_turbulence_isoplanatic("obs.atmos.turbulence.isoplanatic", SyntaxFlag.P,
            "Isoplanatic angle characterising the atmospheric turbulence at telescope site"),
    obs_atmos_water("obs.atmos.water", SyntaxFlag.Q, "Amount of atmospheric water at observing site"),
    obs_atmos_wind("obs.atmos.wind", SyntaxFlag.S, "Related to atmospheric wind at observing site"),
    obs_calib("obs.calib", SyntaxFlag.S, "Calibration observation"),
    obs_calib_flat("obs.calib.flat", SyntaxFlag.S, "Related to flat-field calibration observation (dome, sky, ..)"),
    obs_calib_dark("obs.calib.dark", SyntaxFlag.S, "Related to dark current calibration"),
    obs_exposure("obs.exposure", SyntaxFlag.S, "Exposure"),
    obs_field("obs.field", SyntaxFlag.S, "Region covered by the observation"),
    obs_image("obs.image", SyntaxFlag.S, "Image"),
    obs_observer("obs.observer", SyntaxFlag.Q, "Observer, discoverer"),
    obs_occult("obs.occult", SyntaxFlag.S, "Observation of occultation phenomenon by solar system objects"),
    obs_transit("obs.transit", SyntaxFlag.S, "Observation of transit phenomenon  : exo-planets"),
    obs_param("obs.param", SyntaxFlag.Q, "Various observation or reduction parameter"),
    obs_proposal("obs.proposal", SyntaxFlag.S, "Observation proposal"),
    obs_proposal_cycle("obs.proposal.cycle", SyntaxFlag.Q, "Proposal cycle"),
    obs_sequence("obs.sequence", SyntaxFlag.S, "Sequence of observations, exposures or events"),

    // phot
    phot("phot", SyntaxFlag.E, "Photometry"),
    phot_antennaTemp("phot.antennaTemp", SyntaxFlag.E, "Antenna temperature"),
    phot_calib("phot.calib", SyntaxFlag.Q, "Photometric calibration"),
    phot_color("phot.color", SyntaxFlag.C, "Color index or magnitude difference"),
    phot_color_excess("phot.color.excess", SyntaxFlag.Q, "Color excess"),
    phot_color_reddFree("phot.color.reddFree", SyntaxFlag.Q, "Dereddened color"),
    phot_count("phot.count", SyntaxFlag.E, "Flux expressed in counts"),
    phot_fluence("phot.fluence", SyntaxFlag.E,
            "Radiant photon energy received by a surface per unit area or irradiance of a surface integrated over time of irradiation"),
    phot_flux("phot.flux", SyntaxFlag.E, "Photon flux or irradiance"),
    phot_flux_bol("phot.flux.bol", SyntaxFlag.Q, "Bolometric flux"),
    phot_flux_density("phot.flux.density", SyntaxFlag.E, "Flux density (per wl/freq/energy interval)"),
    phot_flux_density_sb("phot.flux.density.sb", SyntaxFlag.E, "Flux density surface brightness"),
    phot_flux_sb("phot.flux.sb", SyntaxFlag.E, "Flux surface brightness"),
    phot_limbDark("phot.limbDark", SyntaxFlag.E, "Limb-darkening coefficients"),
    phot_mag("phot.mag", SyntaxFlag.E, "Photometric magnitude"),
    phot_mag_bc("phot.mag.bc", SyntaxFlag.E, "Bolometric correction"),
    phot_mag_bol("phot.mag.bol", SyntaxFlag.Q, "Bolometric magnitude"),
    phot_mag_distMod("phot.mag.distMod", SyntaxFlag.Q, "Distance modulus"),
    phot_mag_reddFree("phot.mag.reddFree", SyntaxFlag.E, "Dereddened magnitude"),
    phot_mag_sb("phot.mag.sb", SyntaxFlag.E, "Surface brightness in magnitude units"),
    phot_radiance("phot.radiance", SyntaxFlag.E, "Radiance as energy flux per solid angle"),

    // phys
    phys("phys", SyntaxFlag.Q, "Physical quantities"),
    phys_SFR("phys.SFR", SyntaxFlag.Q, "Star formation rate"),
    phys_absorption("phys.absorption", SyntaxFlag.E, "Extinction or absorption along the line of sight"),
    phys_absorption_coeff("phys.absorption.coeff", SyntaxFlag.Q, "Absorption coefficient (e.g. in a spectral line)"),
    phys_absorption_gal("phys.absorption.gal", SyntaxFlag.Q, "Galactic extinction"),
    phys_absorption_opticalDepth("phys.absorption.opticalDepth", SyntaxFlag.Q, "Optical depth"),
    phys_abund("phys.abund", SyntaxFlag.Q, "Abundance"),
    phys_abund_Fe("phys.abund.Fe", SyntaxFlag.Q, "Fe/H abundance"),
    phys_abund_X("phys.abund.X", SyntaxFlag.Q, "Hydrogen abundance"),
    phys_abund_Y("phys.abund.Y", SyntaxFlag.Q, "Helium abundance"),
    phys_abund_Z("phys.abund.Z", SyntaxFlag.Q, "Metallicity abundance"),
    phys_acceleration("phys.acceleration", SyntaxFlag.Q, "Acceleration"),
    phys_aerosol("phys.aerosol", SyntaxFlag.S, "Relative to aerosol"),
    phys_albedo("phys.albedo", SyntaxFlag.Q, "Albedo or reflectance"),
    phys_angArea("phys.angArea", SyntaxFlag.Q, "Angular area"),
    phys_angMomentum("phys.angMomentum", SyntaxFlag.Q, "Angular momentum"),
    phys_angSize("phys.angSize", SyntaxFlag.E, "Angular size width diameter dimension extension major minor axis extraction radius"),
    phys_angSize_smajAxis("phys.angSize.smajAxis", SyntaxFlag.E, "Angular size extent or extension of semi-major axis"),
    phys_angSize_sminAxis("phys.angSize.sminAxis", SyntaxFlag.E, "Angular size extent or extension of semi-minor axis"),
    phys_area("phys.area", SyntaxFlag.Q, "Area (in surface, not angular units)"),
    phys_atmol("phys.atmol", SyntaxFlag.S, "Atomic and molecular physics (shared properties)"),
    phys_atmol_branchingRatio("phys.atmol.branchingRatio", SyntaxFlag.Q, "Branching ratio"),
    phys_atmol_collisional("phys.atmol.collisional", SyntaxFlag.S, "Related to collisions"),
    phys_atmol_collStrength("phys.atmol.collStrength", SyntaxFlag.Q, "Collisional strength"),
    phys_atmol_configuration("phys.atmol.configuration", SyntaxFlag.Q, "Configuration"),
    phys_atmol_crossSection("phys.atmol.crossSection", SyntaxFlag.Q, "Atomic / molecular cross-section"),
    phys_atmol_element("phys.atmol.element", SyntaxFlag.Q, "Element"),
    phys_atmol_excitation("phys.atmol.excitation", SyntaxFlag.Q, "Atomic molecular excitation parameter"),
    phys_atmol_final("phys.atmol.final", SyntaxFlag.Q, "Quantity refers to atomic/molecular final/ground state, level, etc."),
    phys_atmol_initial("phys.atmol.initial", SyntaxFlag.Q, "Quantity refers to atomic/molecular initial state, level, etc."),
    phys_atmol_ionStage("phys.atmol.ionStage", SyntaxFlag.Q, "Ion, ionization stage"),
    phys_atmol_ionization("phys.atmol.ionization", SyntaxFlag.S, "Related to ionization"),
    phys_atmol_lande("phys.atmol.lande", SyntaxFlag.Q, "Lande factor"),
    phys_atmol_level("phys.atmol.level", SyntaxFlag.S, "Atomic level"),
    phys_atmol_lifetime("phys.atmol.lifetime", SyntaxFlag.Q, "Lifetime of a level"),
    phys_atmol_lineShift("phys.atmol.lineShift", SyntaxFlag.Q, "Line shifting coefficient"),
    phys_atmol_number("phys.atmol.number", SyntaxFlag.Q, "Atomic number Z"),
    phys_atmol_oscStrength("phys.atmol.oscStrength", SyntaxFlag.Q, "Oscillator strength"),
    phys_atmol_parity("phys.atmol.parity", SyntaxFlag.Q, "Parity"),
    phys_atmol_qn("phys.atmol.qn", SyntaxFlag.Q, "Quantum number"),
    phys_atmol_radiationType("phys.atmol.radiationType", SyntaxFlag.Q,
            "Type of radiation characterizing atomic lines (electric dipole/quadrupole, magnetic dipole)"),
    phys_atmol_symmetry("phys.atmol.symmetry", SyntaxFlag.Q, "Type of nuclear spin symmetry"),
    phys_atmol_sWeight("phys.atmol.sWeight", SyntaxFlag.Q, "Statistical weight"),
    phys_atmol_sWeight_nuclear("phys.atmol.sWeight.nuclear", SyntaxFlag.Q, "Statistical weight for nuclear spin states"),
    phys_atmol_term("phys.atmol.term", SyntaxFlag.Q, "Atomic term"),
    phys_atmol_transition("phys.atmol.transition", SyntaxFlag.S, "Transition between states"),
    phys_atmol_transProb("phys.atmol.transProb", SyntaxFlag.Q, "Transition probability, Einstein A coefficient"),
    phys_atmol_wOscStrength("phys.atmol.wOscStrength", SyntaxFlag.Q, "Weighted oscillator strength"),
    phys_atmol_weight("phys.atmol.weight", SyntaxFlag.Q, "Atomic weight"),
    phys_columnDensity("phys.columnDensity", SyntaxFlag.Q, "Column density"),
    phys_composition("phys.composition", SyntaxFlag.S, "Quantities related to composition of objects"),
    phys_composition_massLightRatio("phys.composition.massLightRatio", SyntaxFlag.Q, "Mass to light ratio"),
    phys_composition_yield("phys.composition.yield", SyntaxFlag.Q, "Mass yield"),
    phys_cosmology("phys.cosmology", SyntaxFlag.S, "Related to cosmology"),
    phys_current("phys.current", SyntaxFlag.Q, "Electric current"),
    phys_current_density("phys.current.density", SyntaxFlag.Q, "Electric current density"),
    phys_damping("phys.damping", SyntaxFlag.Q, "Generic damping quantities"),
    phys_density("phys.density", SyntaxFlag.Q, "Density (of mass, electron, ...)"),
    phys_density_phaseSpace("phys.density.phaseSpace", SyntaxFlag.Q, "Density in the phase space"),
    phys_dielectric("phys.dielectric", SyntaxFlag.Q, "Complex dielectric function"),
    phys_dispMeasure("phys.dispMeasure", SyntaxFlag.Q, "Dispersion measure"),
    phys_dust("phys.dust", SyntaxFlag.S, "Relative to dust"),
    phys_electCharge("phys.electCharge", SyntaxFlag.Q, "Electric charge"),
    phys_electField("phys.electField", SyntaxFlag.V, "Electric field"),
    phys_electron("phys.electron", SyntaxFlag.S, "Electron"),
    phys_electron_degen("phys.electron.degen", SyntaxFlag.Q, "Electron degeneracy parameter"),
    phys_emissMeasure("phys.emissMeasure", SyntaxFlag.Q, "Emission measure"),
    phys_emissivity("phys.emissivity", SyntaxFlag.Q, "Emissivity"),
    phys_energy("phys.energy", SyntaxFlag.Q, "Energy"),
    phys_energy_Gibbs("phys.energy.Gibbs", SyntaxFlag.Q, "Gibbs (free) energy or free enthalpy   [ G=H-TS ]"),
    phys_energy_Helmholtz("phys.energy.Helmholtz", SyntaxFlag.Q, "Helmholtz free energy [ A=U-TS ]"),
    phys_energy_density("phys.energy.density", SyntaxFlag.Q, "Energy density"),
    phys_enthalpy("phys.enthalpy", SyntaxFlag.Q, "Enthalpy  [ H=U+pv ]"),
    phys_entropy("phys.entropy", SyntaxFlag.Q, "Entropy"),
    phys_eos("phys.eos", SyntaxFlag.Q, "Equation of state"),
    phys_excitParam("phys.excitParam", SyntaxFlag.Q, "Excitation parameter U"),
    phys_fluence("phys.fluence", SyntaxFlag.E, "Particle energy received  by a surface per unit area and integrated over time"),
    phys_flux("phys.flux", SyntaxFlag.Q, "Flux or flow of particle, energy, etc."),
    phys_flux_energy("phys.flux.energy", SyntaxFlag.Q, "Energy flux, heat flux"),
    phys_gauntFactor("phys.gauntFactor", SyntaxFlag.Q, "Gaunt factor/correction"),
    phys_gravity("phys.gravity", SyntaxFlag.Q, "Gravity"),
    phys_inspiralSpin("phys.inspiralSpin", SyntaxFlag.Q, "Effective inspiral spin in binary mergers (used in GW detections)"),
    phys_ionizParam("phys.ionizParam", SyntaxFlag.Q, "Ionization parameter"),
    phys_ionizParam_coll("phys.ionizParam.coll", SyntaxFlag.Q, "Collisional ionization"),
    phys_ionizParam_rad("phys.ionizParam.rad", SyntaxFlag.Q, "Radiative ionization"),
    phys_luminosity("phys.luminosity", SyntaxFlag.E, "Luminosity"),
    phys_luminosity_fun("phys.luminosity.fun", SyntaxFlag.Q, "Luminosity function"),
    phys_magAbs("phys.magAbs", SyntaxFlag.E, "Absolute magnitude"),
    phys_magAbs_bol("phys.magAbs.bol", SyntaxFlag.Q, "Bolometric absolute magnitude"),
    phys_magField("phys.magField", SyntaxFlag.V, "Magnetic field"),
    phys_mass("phys.mass", SyntaxFlag.Q, "Mass"),
    phys_mass_inertiaMomentum("phys.mass.inertiaMomentum", SyntaxFlag.Q, "Momentum of inertia or rotational inertia"),
    phys_mass_loss("phys.mass.loss", SyntaxFlag.Q, "Mass loss"),
    phys_mol("phys.mol", SyntaxFlag.Q, "Molecular data"),
    phys_mol_dipole("phys.mol.dipole", SyntaxFlag.Q, "Molecular dipole"),
    phys_mol_dipole_elec("phys.mol.dipole.electric", SyntaxFlag.Q, "Molecular electric dipole moment"),
    phys_mol_dipole_mag("phys.mol.dipole.magnetic", SyntaxFlag.Q, "Molecular magnetic dipole moment"),
    phys_mol_dissociation("phys.mol.dissociation", SyntaxFlag.Q, "Molecular dissociation"),
    phys_mol_formationHeat("phys.mol.formationHeat", SyntaxFlag.Q, "Formation heat for molecules"),
    phys_mol_quadrupole("phys.mol.quadrupole", SyntaxFlag.Q, "Molecular quadrupole"),
    phys_mol_quadrupole_elec("phys.mol.quadrupole.electric", SyntaxFlag.Q, "Molecular electric quadrupole moment"),
    phys_mol_rotation("phys.mol.rotation", SyntaxFlag.S, "Molecular rotation"),
    phys_mol_vibration("phys.mol.vibration", SyntaxFlag.S, "Molecular vibration"),
    phys_particle("phys.particle", SyntaxFlag.S, "Related to physical particles"),
    phys_particle_neutrino("phys.particle.neutrino", SyntaxFlag.S, "Related to neutrino"),
    phys_particle_neutron("phys.particle.neutron", SyntaxFlag.S, "Related to neutron"),
    phys_particle_proton("phys.particle.proton", SyntaxFlag.S, "Related to proton"),
    phys_particle_alpha("phys.particle.alpha", SyntaxFlag.S, "Related to alpha particle"),
    phys_phaseSpace("phys.phaseSpace", SyntaxFlag.S, "Related to phase space"),
    phys_polarization("phys.polarization", SyntaxFlag.E, "Polarization degree (or percentage)"),
    phys_polarization_circular("phys.polarization.circular", SyntaxFlag.Q, "Circular polarization"),
    phys_polarization_coherency("phys.polarization.coherency", SyntaxFlag.Q, "Matrix of the correlation between components of an electromagnetic wave"),
    phys_polarization_linear("phys.polarization.linear", SyntaxFlag.Q, "Linear polarization"),
    phys_polarization_rotMeasure("phys.polarization.rotMeasure", SyntaxFlag.Q, "Rotation measure polarization"),
    phys_polarization_stokes("phys.polarization.stokes", SyntaxFlag.Q, "Stokes polarization"),
    phys_polarization_stokes_I("phys.polarization.stokes.I", SyntaxFlag.Q, "Stokes polarization coefficient I"),
    phys_polarization_stokes_Q("phys.polarization.stokes.Q", SyntaxFlag.Q, "Stokes polarization coefficient Q"),
    phys_polarization_stokes_U("phys.polarization.stokes.U", SyntaxFlag.Q, "Stokes polarization coefficient U"),
    phys_polarization_stokes_V("phys.polarization.stokes.V", SyntaxFlag.Q, "Stokes polarization coefficient V"),
    phys_potential("phys.potential", SyntaxFlag.Q, "Potential (electric, gravitational, etc.)"),
    phys_pressure("phys.pressure", SyntaxFlag.Q, "Pressure"),
    phys_recombination_coeff("phys.recombination.coeff", SyntaxFlag.Q, "Recombination coefficient"),
    phys_reflectance("phys.reflectance", SyntaxFlag.Q, "Radiance factor (received radiance divided by input radiance)"),
    phys_reflectance_bidirectional("phys.reflectance.bidirectional", SyntaxFlag.Q, "Bidirectional reflectance"),
    phys_reflectance_bidirectional_df("phys.reflectance.bidirectional.df", SyntaxFlag.Q, "Bidirectional reflectance distribution function"),
    phys_reflectance_factor("phys.reflectance.factor", SyntaxFlag.Q, "Reflectance normalized per direction cosine of incidence angle"),
    phys_refractIndex("phys.refractIndex", SyntaxFlag.Q, "Refraction index"),
    phys_size("phys.size", SyntaxFlag.Q, "Linear size, length (not angular) in length units or bytes"),
    phys_size_axisRatio("phys.size.axisRatio", SyntaxFlag.Q, "Axis ratio (a/b) or (b/a)"),
    phys_size_diameter("phys.size.diameter", SyntaxFlag.Q, "Diameter"),
    phys_size_radius("phys.size.radius", SyntaxFlag.Q, "Radius"),
    phys_size_smajAxis("phys.size.smajAxis", SyntaxFlag.Q, "Linear semi major axis"),
    phys_size_sminAxis("phys.size.sminAxis", SyntaxFlag.Q, "Linear semi minor axis"),
    phys_size_smedAxis("phys.size.smedAxis", SyntaxFlag.Q, "Linear semi median axis for 3D ellipsoids"),
    phys_temperature("phys.temperature", SyntaxFlag.Q, "Temperature"),
    phys_temperature_dew("phys.temperature.dew", SyntaxFlag.Q, "Dew temperature measured at the telescope site during an observation"),
    phys_temperature_effective("phys.temperature.effective", SyntaxFlag.Q, "Effective temperature"),
    phys_temperature_electron("phys.temperature.electron", SyntaxFlag.Q, "Electron temperature"),
    phys_transmission("phys.transmission", SyntaxFlag.Q, "Transmission (of filter, instrument, ...)"),
    phys_veloc("phys.veloc", SyntaxFlag.V, "Space velocity"),
    phys_veloc_ang("phys.veloc.ang", SyntaxFlag.Q, "Angular velocity"),
    phys_veloc_dispersion("phys.veloc.dispersion", SyntaxFlag.Q, "Velocity dispersion"),
    phys_veloc_escape("phys.veloc.escape", SyntaxFlag.Q, "Escape velocity"),
    phys_veloc_expansion("phys.veloc.expansion", SyntaxFlag.Q, "Expansion velocity"),
    phys_veloc_microTurb("phys.veloc.microTurb", SyntaxFlag.Q, "Microturbulence velocity"),
    phys_veloc_orbital("phys.veloc.orbital", SyntaxFlag.Q, "Orbital velocity"),
    phys_veloc_pulsat("phys.veloc.pulsat", SyntaxFlag.Q, "Pulsational velocity"),
    phys_veloc_rotat("phys.veloc.rotat", SyntaxFlag.Q, "Rotational velocity"),
    phys_veloc_transverse("phys.veloc.transverse", SyntaxFlag.Q, "Transverse / tangential velocity"),
    phys_virial("phys.virial", SyntaxFlag.S, "Related to virial quantities (mass, radius, ...)"),
    phys_volume("phys.volume", SyntaxFlag.Q, "Volume (in cubic units)"),
    phys_voltage("phys.voltage", SyntaxFlag.Q, "Electric potential difference over a distance or measured by an instrument"),

    // pos
    pos("pos", SyntaxFlag.Q, "Position and coordinates"),
    pos_angDistance("pos.angDistance", SyntaxFlag.Q, "Angular distance, elongation"),
    pos_angResolution("pos.angResolution", SyntaxFlag.Q, "Angular resolution"),
    pos_az("pos.az", SyntaxFlag.Q, "Position in alt-azimuthal frame"),
    pos_az_alt("pos.az.alt", SyntaxFlag.Q, "Alt-azimuthal altitude"),
    pos_az_azi("pos.az.azi", SyntaxFlag.Q, "Alt-azimuthal azimuth"),
    pos_az_zd("pos.az.zd", SyntaxFlag.Q, "Alt-azimuthal zenith distance"),
    pos_azimuth("pos.azimuth", SyntaxFlag.Q, "Azimuthal angle in a generic reference plane"),
    pos_barycenter("pos.barycenter", SyntaxFlag.S, "Barycenter"),
    pos_bodycentric("pos.bodycentric", SyntaxFlag.S, "Body-centric related coordinate"),
    pos_bodygraphic("pos.bodygraphic", SyntaxFlag.S, "Body-graphic related coordinate"),
    pos_bodyrc("pos.bodyrc", SyntaxFlag.S, "Body related coordinates"),
    pos_bodyrc_alt("pos.bodyrc.alt", SyntaxFlag.Q, "Body related coordinate (altitude on the body)"),
    pos_bodyrc_lat("pos.bodyrc.lat", SyntaxFlag.Q, "Body related coordinate (latitude on the body)"),
    pos_bodyrc_lon("pos.bodyrc.lon", SyntaxFlag.Q, "Body related coordinate (longitude on the body)"),
    pos_cartesian("pos.cartesian", SyntaxFlag.S, "Cartesian (rectangular) coordinates"),
    pos_cartesian_x("pos.cartesian.x", SyntaxFlag.Q, "Cartesian coordinate along the x-axis"),
    pos_cartesian_y("pos.cartesian.y", SyntaxFlag.Q, "Cartesian coordinate along the y-axis"),
    pos_cartesian_z("pos.cartesian.z", SyntaxFlag.Q, "Cartesian coordinate along the z-axis"),
    pos_centroid("pos.centroid", SyntaxFlag.S, "Related to centroid position"),
    pos_cmb("pos.cmb", SyntaxFlag.S, "Cosmic Microwave Background reference frame"),
    pos_cylindrical("pos.cylindrical", SyntaxFlag.S, "Related to cylindrical coordinates"),
    pos_cylindrical_azi("pos.cylindrical.azi", SyntaxFlag.Q, "Azimuthal angle around z-axis (cylindrical coordinates)"),
    pos_cylindrical_r("pos.cylindrical.r", SyntaxFlag.Q, "Radial distance from z-axis (cylindrical coordinates)"),
    pos_cylindrical_z("pos.cylindrical.z", SyntaxFlag.Q, "Height or altitude from reference plane (cylindrical coordinates)"),
    pos_dirCos("pos.dirCos", SyntaxFlag.Q, "Direction cosine"),
    pos_distance("pos.distance", SyntaxFlag.V, "Linear distance"),
    pos_earth("pos.earth", SyntaxFlag.S, "Coordinates related to Earth"),
    pos_earth_altitude("pos.earth.altitude", SyntaxFlag.Q, "Altitude, height on Earth  above sea level"),
    pos_earth_lat("pos.earth.lat", SyntaxFlag.Q, "Latitude on Earth"),
    pos_earth_lon("pos.earth.lon", SyntaxFlag.Q, "Longitude on Earth"),
    pos_ecliptic("pos.ecliptic", SyntaxFlag.S, "Ecliptic coordinates"),
    pos_ecliptic_lat("pos.ecliptic.lat", SyntaxFlag.Q, "Ecliptic latitude"),
    pos_ecliptic_lon("pos.ecliptic.lon", SyntaxFlag.Q, "Ecliptic longitude"),
    pos_emergenceAng("pos.emergenceAng", SyntaxFlag.Q, "Emergence angle of optical ray on an interface"),
    pos_eop("pos.eop", SyntaxFlag.S, "Earth orientation parameters"),
    pos_ephem("pos.ephem", SyntaxFlag.Q, "Ephemeris"),
    pos_eq("pos.eq", SyntaxFlag.Q, "Equatorial coordinates"),
    pos_eq_dec("pos.eq.dec", SyntaxFlag.Q, "Declination in equatorial coordinates"),
    pos_eq_ha("pos.eq.ha", SyntaxFlag.Q, "Hour-angle"),
    pos_eq_ra("pos.eq.ra", SyntaxFlag.Q, "Right ascension in equatorial coordinates"),
    pos_eq_spd("pos.eq.spd", SyntaxFlag.Q, "South polar distance in equatorial coordinates"),
    pos_errorEllipse("pos.errorEllipse", SyntaxFlag.S, "Positional error ellipse"),
    pos_frame("pos.frame", SyntaxFlag.Q, "Reference frame used for positions"),
    pos_galactic("pos.galactic", SyntaxFlag.S, "Galactic coordinates"),
    pos_galactic_lat("pos.galactic.lat", SyntaxFlag.Q, "Latitude in galactic coordinates"),
    pos_galactic_lon("pos.galactic.lon", SyntaxFlag.Q, "Longitude in galactic coordinates"),
    pos_galactocentric("pos.galactocentric", SyntaxFlag.S, "Galactocentric coordinate system"),
    pos_geocentric("pos.geocentric", SyntaxFlag.S, "Geocentric coordinate system"),
    pos_healpix("pos.healpix", SyntaxFlag.Q, "Hierarchical Equal Area IsoLatitude Pixelization"),
    pos_heliocentric("pos.heliocentric", SyntaxFlag.S, "Heliocentric position coordinate (solar system bodies)"),
    pos_HTM("pos.HTM", SyntaxFlag.Q, "Hierarchical Triangular Mesh"),
    pos_incidenceAng("pos.incidenceAng", SyntaxFlag.Q, "Incidence angle of optical ray on an interface"),
    pos_lg("pos.lg", SyntaxFlag.S, "Local Group reference frame"),
    pos_lsr("pos.lsr", SyntaxFlag.S, "Local Standard of Rest reference frame"),
    pos_lunar("pos.lunar", SyntaxFlag.Q, "Lunar coordinates"),
    pos_lunar_occult("pos.lunar.occult", SyntaxFlag.Q, "Occultation by lunar limb"),
    pos_nutation("pos.nutation", SyntaxFlag.Q, "Nutation (of a body)"),
    pos_outline("pos.outline", SyntaxFlag.Q, "Set of points outlining a region (contour)"),
    pos_parallax("pos.parallax", SyntaxFlag.Q, "Parallax"),
    pos_parallax_dyn("pos.parallax.dyn", SyntaxFlag.Q, "Dynamical parallax"),
    pos_parallax_phot("pos.parallax.phot", SyntaxFlag.Q, "Photometric parallaxes"),
    pos_parallax_spect("pos.parallax.spect", SyntaxFlag.Q, "Spectroscopic parallax"),
    pos_parallax_trig("pos.parallax.trig", SyntaxFlag.Q, "Trigonometric parallax"),
    pos_phaseAng("pos.phaseAng", SyntaxFlag.Q, "Phase angle, e.g. elongation of earth from sun as seen from a third celestial object"),
    pos_pm("pos.pm", SyntaxFlag.V, "Proper motion"),
    pos_posAng("pos.posAng", SyntaxFlag.Q, "Position angle of a given vector"),
    pos_precess("pos.precess", SyntaxFlag.V, "Precession (in equatorial coordinates)"),
    pos_projection("pos.projection", SyntaxFlag.Q, "2D spatial projection"),
    pos_resolution("pos.resolution", SyntaxFlag.Q, "Spatial linear resolution (not angular)"),
    pos_spherical("pos.spherical", SyntaxFlag.S, "Related to spherical coordinates"),
    pos_spherical_azi("pos.spherical.azi", SyntaxFlag.Q, "Azimuthal angle (spherical coordinates)"),
    pos_spherical_colat("pos.spherical.colat", SyntaxFlag.Q, "Polar or Colatitude angle (spherical coordinates)"),
    pos_spherical_r("pos.spherical.r", SyntaxFlag.Q, "Radial distance or radius (spherical coordinates)"),
    pos_supergalactic("pos.supergalactic", SyntaxFlag.S, "Supergalactic coordinates"),
    pos_supergalactic_lat("pos.supergalactic.lat", SyntaxFlag.Q, "Latitude in supergalactic coordinates"),
    pos_supergalactic_lon("pos.supergalactic.lon", SyntaxFlag.Q, "Longitude in supergalactic coordinates"),
    pos_wcs("pos.wcs", SyntaxFlag.P, "WCS keywords"),
    pos_wcs_cdmatrix("pos.wcs.cdmatrix", SyntaxFlag.P, "WCS CDMATRIX"),
    pos_wcs_crpix("pos.wcs.crpix", SyntaxFlag.P, "WCS CRPIX"),
    pos_wcs_crval("pos.wcs.crval", SyntaxFlag.P, "WCS CRVAL"),
    pos_wcs_ctype("pos.wcs.ctype", SyntaxFlag.P, "WCS CTYPE"),
    pos_wcs_naxes("pos.wcs.naxes", SyntaxFlag.P, "WCS NAXES"),
    pos_wcs_naxis("pos.wcs.naxis", SyntaxFlag.P, "WCS NAXIS"),
    pos_wcs_scale("pos.wcs.scale", SyntaxFlag.P, "WCS scale or scale of an image"),

    // spect
    spect("spect", SyntaxFlag.Q, "Spectroscopy"),
    spect_binSize("spect.binSize", SyntaxFlag.Q, "Spectral bin size"),
    spect_continuum("spect.continuum", SyntaxFlag.S, "Continuum spectrum"),
    spect_dopplerParam("spect.dopplerParam", SyntaxFlag.Q, "Doppler parameter b"),
    spect_dopplerVeloc("spect.dopplerVeloc", SyntaxFlag.E, "Radial velocity, derived from the shift of some spectral feature"),
    spect_dopplerVeloc_opt("spect.dopplerVeloc.opt", SyntaxFlag.E, "Radial velocity derived from a wavelength shift using the optical convention"),
    spect_dopplerVeloc_radio("spect.dopplerVeloc.radio", SyntaxFlag.E, "Radial velocity derived from a frequency shift using the radio convention"),
    spect_index("spect.index", SyntaxFlag.E, "Spectral index"),
    spect_line("spect.line", SyntaxFlag.S, "Spectral line"),
    spect_line_asymmetry("spect.line.asymmetry", SyntaxFlag.E, "Line asymmetry"),
    spect_line_broad("spect.line.broad", SyntaxFlag.E, "Spectral line broadening"),
    spect_line_broad_Stark("spect.line.broad.Stark", SyntaxFlag.Q, "Stark line broadening coefficient"),
    spect_line_broad_Zeeman("spect.line.broad.Zeeman", SyntaxFlag.E, "Zeeman broadening"),
    spect_line_eqWidth("spect.line.eqWidth", SyntaxFlag.E, "Line equivalent width"),
    spect_line_intensity("spect.line.intensity", SyntaxFlag.E, "Line intensity"),
    spect_line_profile("spect.line.profile", SyntaxFlag.E, "Line profile"),
    spect_line_strength("spect.line.strength", SyntaxFlag.Q, "Spectral line strength S"),
    spect_line_width("spect.line.width", SyntaxFlag.E, "Spectral line full width half maximum"),
    spect_resolution("spect.resolution", SyntaxFlag.Q, "Spectral (or velocity) resolution"),

    // src
    src("src", SyntaxFlag.S, "Observed source viewed on the sky"),
    src_calib("src.calib", SyntaxFlag.S, "Calibration source"),
    src_calib_guideStar("src.calib.guideStar", SyntaxFlag.S, "Guide star"),
    src_class("src.class", SyntaxFlag.Q, "Source classification (star, galaxy, cluster, comet, asteroid )"),
    src_class_color("src.class.color", SyntaxFlag.Q, "Color classification"),
    src_class_distance("src.class.distance", SyntaxFlag.Q, "Distance class e.g. Abell"),
    src_class_luminosity("src.class.luminosity", SyntaxFlag.Q, "Luminosity class"),
    src_class_richness("src.class.richness", SyntaxFlag.Q, "Richness class e.g. Abell"),
    src_class_starGalaxy("src.class.starGalaxy", SyntaxFlag.Q, "Star/galaxy discriminator, stellarity index"),
    src_class_struct("src.class.struct", SyntaxFlag.Q, "Structure classification e.g. Bautz-Morgan"),
    src_density("src.density", SyntaxFlag.Q, "Density of sources"),
    src_ellipticity("src.ellipticity", SyntaxFlag.Q, "Source ellipticity"),
    src_impactParam("src.impactParam", SyntaxFlag.Q, "Impact parameter"),
    src_morph("src.morph", SyntaxFlag.Q, "Morphology structure"),
    src_morph_param("src.morph.param", SyntaxFlag.Q, "Morphological parameter"),
    src_morph_scLength("src.morph.scLength", SyntaxFlag.Q, "Scale length for a galactic component (disc or bulge)"),
    src_morph_type("src.morph.type", SyntaxFlag.Q, "Hubble morphological type (galaxies)"),
    src_net("src.net", SyntaxFlag.S, "Qualifier indicating that a quantity (e.g. flux) is background subtracted rather than total"),
    src_orbital("src.orbital", SyntaxFlag.Q, "Orbital parameters"),
    src_orbital_eccentricity("src.orbital.eccentricity", SyntaxFlag.Q, "Orbit eccentricity"),
    src_orbital_inclination("src.orbital.inclination", SyntaxFlag.Q, "Orbit inclination"),
    src_orbital_meanAnomaly("src.orbital.meanAnomaly", SyntaxFlag.Q, "Orbit mean anomaly"),
    src_orbital_meanMotion("src.orbital.meanMotion", SyntaxFlag.Q, "Mean motion"),
    src_orbital_node("src.orbital.node", SyntaxFlag.Q, "Ascending node"),
    src_orbital_periastron("src.orbital.periastron", SyntaxFlag.Q, "Periastron"),
    src_orbital_Tisserand("src.orbital.Tisserand", SyntaxFlag.Q, "Tisserand parameter (generic)"),
    src_orbital_TissJ("src.orbital.TissJ", SyntaxFlag.Q, "Tisserand parameter with respect to Jupiter"),
    src_redshift("src.redshift", SyntaxFlag.Q, "Redshift"),
    src_redshift_phot("src.redshift.phot", SyntaxFlag.Q, "Photometric redshift"),
    src_sample("src.sample", SyntaxFlag.Q, "Sample"),
    src_spType("src.spType", SyntaxFlag.Q, "Spectral type MK"),
    src_var("src.var", SyntaxFlag.Q, "Variability of source"),
    src_var_amplitude("src.var.amplitude", SyntaxFlag.Q, "Amplitude of variation"),
    src_var_index("src.var.index", SyntaxFlag.Q, "Variability index"),
    src_var_pulse("src.var.pulse", SyntaxFlag.Q, "Pulse"),

    // stat
    stat("stat", SyntaxFlag.Q, "Statistical parameters"),
    stat_asymmetry("stat.asymmetry", SyntaxFlag.Q, "Measure of asymmetry"),
    stat_confidenceLevel("stat.confidenceLevel", SyntaxFlag.P, "Level of confidence for a detection or classification process"),
    stat_correlation("stat.correlation", SyntaxFlag.P, "Correlation between two parameters"),
    stat_covariance("stat.covariance", SyntaxFlag.P, "Covariance between two parameters"),
    stat_error("stat.error", SyntaxFlag.P, "Statistical error"),
    stat_error_sys("stat.error.sys", SyntaxFlag.P, "Systematic error"),
    stat_falsePositive("stat.falsePositive", SyntaxFlag.Q, "Rate of false alarm in detection problems"),
    stat_falseNegative("stat.falseNegative", SyntaxFlag.Q, "Rate of missed or false negative detection"),
    stat_filling("stat.filling", SyntaxFlag.Q, "Filling factor (volume, time, ...)"),
    stat_fit("stat.fit", SyntaxFlag.Q, "Fit"),
    stat_fit_chi2("stat.fit.chi2", SyntaxFlag.P, "Chi2"),
    stat_fit_dof("stat.fit.dof", SyntaxFlag.P, "Degrees of freedom"),
    stat_fit_goodness("stat.fit.goodness", SyntaxFlag.P, "Goodness or significance of fit"),
    stat_fit_omc("stat.fit.omc", SyntaxFlag.S, "Observed minus computed"),
    stat_fit_param("stat.fit.param", SyntaxFlag.Q, "Parameter of fit"),
    stat_fit_residual("stat.fit.residual", SyntaxFlag.P, "Residual fit"),
    stat_Fourier("stat.Fourier", SyntaxFlag.Q, "Fourier coefficient"),
    stat_Fourier_amplitude("stat.Fourier.amplitude", SyntaxFlag.Q, "Amplitude of Fourier coefficient"),
    stat_fwhm("stat.fwhm", SyntaxFlag.S, "Full width at half maximum"),
    stat_interval("stat.interval", SyntaxFlag.S, "Generic interval between two limits (defined as a pair of values)"),
    stat_kurtosis("stat.kurtosis", SyntaxFlag.P, "Kurtosis of a probability distribution (Fourth moment)"),
    stat_likelihood("stat.likelihood", SyntaxFlag.P, "Likelihood"),
    stat_mad("stat.mad", SyntaxFlag.P, "Median absolute deviation from median value in a univariate data sample"),
    stat_max("stat.max", SyntaxFlag.S, "Maximum or upper limit"),
    stat_mean("stat.mean", SyntaxFlag.S, "Mean, average value"),
    stat_median("stat.median", SyntaxFlag.S, "Median value"),
    stat_min("stat.min", SyntaxFlag.S, "Minimum or lowest limit"),
    stat_param("stat.param", SyntaxFlag.Q, "Parameter"),
    stat_percentile("stat.percentile", SyntaxFlag.Q, "Percentile of a statistical distribution"),
    stat_probability("stat.probability", SyntaxFlag.Q, "Probability"),
    stat_rank("stat.rank", SyntaxFlag.P, "Rank or order in list of sorted values"),
    stat_rms("stat.rms", SyntaxFlag.P, "Root mean square as square root of sum of squared values or quadratic mean"),
    stat_skewness("stat.skewness", SyntaxFlag.P, "Skewness of a probability distribution (third moment)"),
    stat_snr("stat.snr", SyntaxFlag.P, "Signal to noise ratio"),
    stat_stdev("stat.stdev", SyntaxFlag.Q, "Standard deviation as the square root of the variance"),
    stat_uncalib("stat.uncalib", SyntaxFlag.S, "Qualifier of a generic uncalibrated quantity"),
    stat_value("stat.value", SyntaxFlag.Q, "Miscellaneous value"),
    stat_variance("stat.variance", SyntaxFlag.P, "Variance"),
    stat_weight("stat.weight", SyntaxFlag.P, "Statistical weight"),

    // time
    time("time", SyntaxFlag.Q, "Time, generic quantity in units of time or date"),
    time_age("time.age", SyntaxFlag.Q, "Age"),
    time_creation("time.creation", SyntaxFlag.Q, "Creation time/date (of dataset, file, catalogue,...)"),
    time_crossing("time.crossing", SyntaxFlag.Q, "Crossing time"),
    time_duration("time.duration", SyntaxFlag.Q, "Interval of time describing the duration of a generic event or phenomenon"),
    time_end("time.end", SyntaxFlag.Q, "End time/date of a generic event"),
    time_epoch("time.epoch", SyntaxFlag.Q, "Instant of time related to a generic event (epoch, date, Julian date, time stamp/tag,...)"),
    time_equinox("time.equinox", SyntaxFlag.Q, "Equinox"),
    time_interval("time.interval", SyntaxFlag.Q, "Time interval, time-bin, time elapsed between two events, not the duration of an event"),
    time_lifetime("time.lifetime", SyntaxFlag.Q, "Lifetime"),
    time_period("time.period", SyntaxFlag.Q, "Period, interval of time between the recurrence of phases in a periodic phenomenon"),
    time_period_pulsation("time.period.pulsation", SyntaxFlag.Q, "Period of pulsation or oscillation of a body or a phenomenon"),
    time_period_revolution("time.period.revolution", SyntaxFlag.Q, "Period of revolution of a body around a primary one (similar to year)"),
    time_period_rotation("time.period.rotation", SyntaxFlag.Q, "Period of rotation of a body around its axis (similar to day)"),
    time_phase("time.phase", SyntaxFlag.Q, "Phase, position within a period"),
    time_processing("time.processing", SyntaxFlag.Q, "A time/date associated with the processing of data"),
    time_publiYear("time.publiYear", SyntaxFlag.Q, "Publication year"),
    time_relax("time.relax", SyntaxFlag.Q, "Relaxation time"),
    time_release("time.release", SyntaxFlag.Q, "The time/date data is available to the public"),
    time_resolution("time.resolution", SyntaxFlag.Q, "Time resolution"),
    time_scale("time.scale", SyntaxFlag.Q, "Timescale"),
    time_start("time.start", SyntaxFlag.Q, "Start time/date of generic event");

    private final String word;
    private final SyntaxFlag flag;
    private final String description;

    UCDWordEntry(String word, SyntaxFlag flag, String description) {
        this.word = word;
        this.flag = flag;
        this.description = description;
    }

    public String getWord() {
        return word;
    }

    public SyntaxFlag getFlag() {
        return flag;
    }

    public String getDescription() {
        return description;
    }

    public UCDWord toUcdWord() {
        return new UCDWord(word, flag, description);
    }

    /**
     * Looks up an entry by the official word string (case-insensitive).
     *
     * @param word the UCD atom to look up
     * @return the matching entry, or {@code null} if not found
     */
    public static UCDWordEntry fromWord(String word) {
        if (word == null) {
            return null;
        }
        String lower = word.toLowerCase();
        for (UCDWordEntry entry : values()) {
            if (entry.word.toLowerCase().equals(lower)) {
                return entry;
            }
        }
        return null;
    }
}