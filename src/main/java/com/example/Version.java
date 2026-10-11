package com.example;

public final class Version {

    private Version() {
    }

    /** Version from the jar manifest, or "unknown" when running from unpackaged classes. */
    public static String get() {
        String version = Version.class.getPackage().getImplementationVersion();
        return version != null ? version : "unknown";
    }
}
