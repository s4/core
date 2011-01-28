package io.s4.client;

/**
 * Client's write mode.
 */
public enum ClientWriteMode {
    Enabled, Disabled;

    public static ClientWriteMode fromString(String s) {
        if (s.equalsIgnoreCase("enabled"))
            return Enabled;
        else if (s.equalsIgnoreCase("disabled"))
            return Disabled;
        else
            return null;
    }
}