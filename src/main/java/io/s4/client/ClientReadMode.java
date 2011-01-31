package io.s4.client;

/**
 * Client mode.
 */
public enum ClientReadMode {
    None(false, false), Private(true, false), Select(true, true), All(true, true);

    private final boolean priv;
    private final boolean pub;

    ClientReadMode(boolean priv, boolean pub) {
        this.priv = priv;
        this.pub = pub;
    }

    public boolean takePublic() {
        return pub;
    }

    public boolean takePrivate() {
        return priv;
    }

    public static ClientReadMode fromString(String s) {
        if (s.equalsIgnoreCase("none"))
            return None;
        else if (s.equalsIgnoreCase("private"))
            return Private;
        else if (s.equalsIgnoreCase("select"))
            return Select;
        else if (s.equalsIgnoreCase("all"))
            return All;
        else
            return null;
    }
}