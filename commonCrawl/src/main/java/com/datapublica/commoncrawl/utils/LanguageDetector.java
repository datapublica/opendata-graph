package com.datapublica.commoncrawl.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.io.IOUtils;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 * Creates and configures statically a DetectorFactory and loads the languages profiles into it. Used to avoid
 * reconfiguring the DetectorFactory each time a Detector needs to be created and also used to create a Detector
 */
public class LanguageDetector {

    private final static Log LOG = LogFactory.getLog(LanguageDetector.class);

    // private static final String[] mostCommonLanguagesToFrench = new String[] { "en", "es", "fr", "it" };

    /**
     * The threshold distance value to help the detector decide which language
     */
    private static final double THRESHOLD = 0.5;

    static {

        // Initialize logger
        Loggers.setup();

        // Load the language profiles (n-grams lists for each language)
        LOG.info("Loading language profiles");
        loadProfiles();
        DetectorFactory.setSeed(0);
    }

    /**
     * Creates a Detector using the configured {@link DetectorFactory} initialized and configured statically in this class
     * 
     * @return {@link Detector} the instantiated detector
     */
    public static Detector createDetector() {
        try {
            return DetectorFactory.create(THRESHOLD);
        } catch (LangDetectException e) {
            LOG.error("Cannot create Detector", e);
            return null;
        }
    }

    /**
     * This method iterates over all the languages list supported by this detector and loads them all to initialize the
     * {@link DetectorFactory}
     */
    public static void loadProfiles() {

        List<String> allLangCodes = JobHelper.getListFromResource("profiles-list.txt");
        List<String> json_profiles = new ArrayList<String>();
        for (String languageCode : allLangCodes) {
            try {
                json_profiles.add(getLanguageProfile(languageCode));
            } catch (IOException e) {
                LOG.info("Can't load language profile for " + languageCode);
            }
        }

        // Finally load all the profiles
        try {
            DetectorFactory.loadProfile(json_profiles);
        } catch (LangDetectException e) {
            // Error if files could not be accessed or their format is correct (JSON)
            LOG.error("Cannot load profiles", e);
        }
    }

    /**
     * Gets a language profile as a JSON string by loading it from its individual file. The language files are bundled
     * in the lang-detect library jar
     * 
     * @param langCode ISO code of the language
     * @return json language profile as a string
     * @throws IOException if the profile file cannot be accessed
     */
    private static String getLanguageProfile(String langCode) throws IOException {
        InputStream iStream = DetectorFactory.class.getClassLoader().getResourceAsStream("profiles/" + langCode);
        String profile = IOUtils.toString(iStream);
        IOUtils.closeQuietly(iStream);
        return profile;
    }
}
