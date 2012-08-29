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
 * configuring the DetectorFactory each time a Detector needs to be created and also used to create a Detector
 */
public class LanguageDetector {

    private final static Log LOG = LogFactory.getLog(LanguageDetector.class);

    // private static final String[] mostCommonLanguagesToFrench = new String[] { "en", "es", "fr", "it" };

    /**
     * The threshold distance value to help the detector decide which language
     */
    public static final double THRESHOLD = 0.5;

    static {

        Loggers.setup();

        // Load the languages profiles (n-grams lists for each language)
        LOG.info("Loading language profiles");
        loadProfiles();
        DetectorFactory.setSeed(0);
    }

    /**
     * Creates a Detector using the configured DetectorFactory initiated and configured statically in this class
     * 
     * @return Detector the instantiated detector
     */
    public static Detector createDetector() {
        try {
            return DetectorFactory.create(THRESHOLD);
        } catch (LangDetectException e) {
            LOG.error("Cannot create Detector", e);
        }
        return null;
    }

    /**
     * Loads the languages profiles which are bundled in a folder in the jar. Each profile is a separate JSON file named
     * with the language ISO code and containing n-grams model for the language. This method iterates over all the
     * languages list and adds each profile individually by passing the profile as a file argument. Because of the Jar
     * file access limit, files are extracted to the system temporary directory then used to load profiles.
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

        // Finally load the profiles
        // Unfortunately this function reads the passed files as an InputStream but doesn't take InputStream as an
        // argument. Otherwise we could directly read the profiles from the jar
        try {
            DetectorFactory.loadProfile(json_profiles);
        } catch (LangDetectException e) {
            // Error if files could not be accessed or formats aren't correct (JSON)
            LOG.error("Cannot load profiles", e);
        }
    }

    private static String getLanguageProfile(String langCode) throws IOException {
        InputStream iStream = DetectorFactory.class.getClassLoader().getResourceAsStream("profiles/" + langCode);
        String profile = IOUtils.toString(iStream);
        IOUtils.closeQuietly(iStream);
        return profile;
    }
}
