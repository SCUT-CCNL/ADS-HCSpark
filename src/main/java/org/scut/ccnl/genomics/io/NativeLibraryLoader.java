package org.scut.ccnl.genomics.io;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.scut.ccnl.genomics.SparkHC;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;


/**
 * Loads native libraries from the classpath, usually from a jar file.
 */
public final class NativeLibraryLoader {
    private static final Logger logger = Logger.getLogger(NativeLibraryLoader.class);
    private static final String USE_LIBRARY_PATH = "USE_LIBRARY_PATH";
    private static final Set<String> loadedLibraries = new HashSet<String>();

    /**
     * Tries to load the native library from the classpath, usually from a jar file. <p>
     *
     * If the USE_LIBRARY_PATH environment variable is defined, the native library will be loaded from the
     * java.library.path instead of the classpath.
     *
     * @param tempDir  directory where the native library is extracted or null to use the system temp directory
     * @param libraryName  name of the shared library without system dependent modifications
     * @return true if the library was loaded successfully, false otherwise
     */
    public static synchronized boolean load(File tempDir, String libraryName) throws Exception {
        if (loadedLibraries.contains(libraryName)) {
            return true;
        }

        final String systemLibraryName = libraryName;

        // load from the java classpath
        final String resourcePath =  systemLibraryName;
        final URL inputUrl = SparkHC.class.getResource(resourcePath);
        if (inputUrl == null) {
            logger.warn("Unable to find native library: " + resourcePath);
            return false;
        }
        logger.info(String.format("Loading %s from %s", systemLibraryName, inputUrl.toString()));

        try {
            final File temp = File.createTempFile(FilenameUtils.getBaseName(resourcePath),
                    "." + FilenameUtils.getExtension(resourcePath), tempDir);
            FileUtils.copyURLToFile(inputUrl, temp);
            temp.deleteOnExit();
            logger.debug(String.format("Extracting %s to %s", systemLibraryName, temp.getAbsolutePath()));
            System.load(temp.getAbsolutePath());
        } catch (Exception|Error e) {
            logger.warn(String.format("Unable to load %s from %s", systemLibraryName, resourcePath));
            throw e;
        }

        loadedLibraries.add(libraryName);
        return true;
    }
}