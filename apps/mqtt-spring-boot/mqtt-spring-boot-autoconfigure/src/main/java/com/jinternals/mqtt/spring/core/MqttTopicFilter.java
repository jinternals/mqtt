package com.jinternals.mqtt.spring.core;

/** Topic-filter arithmetic: does a filter match a topic, and can two filters match the same topic. */
public final class MqttTopicFilter {

    private MqttTopicFilter() {}

    /** MQTT topic-filter matching: {@code +} spans one level, {@code #} spans the rest. */
    public static boolean matches(String filter, String topic) {
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        for (int i = 0; i < f.length; i++) {
            if (f[i].equals("#")) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!f[i].equals("+") && !f[i].equals(t[i])) {
                return false;
            }
        }
        return f.length == t.length;
    }

    /**
     * Whether some topic exists that both filters would match.
     *
     * <p>This is what decides whether two listeners can be given different acknowledgement modes.
     * An acknowledgement applies to a message, not to a subscription, so two listeners that can
     * ever see the same message cannot disagree about who acknowledges it.
     *
     * <p>Answered structurally rather than by enumerating topics: walk both filters together, where
     * {@code +} matches any single level and {@code #} absorbs everything remaining.
     */
    public static boolean overlap(String filterA, String filterB) {
        return overlap(filterA.split("/", -1), 0, filterB.split("/", -1), 0);
    }

    private static boolean overlap(String[] a, int i, String[] b, int j) {
        while (true) {
            boolean aExhausted = i == a.length;
            boolean bExhausted = j == b.length;

            if (aExhausted && bExhausted) {
                return true;
            }
            // One filter ran out of levels. The other can still agree only if what remains is a
            // trailing '#', which also matches the parent level and therefore zero further levels.
            if (aExhausted) {
                return b[j].equals("#") && j == b.length - 1;
            }
            if (bExhausted) {
                return a[i].equals("#") && i == a.length - 1;
            }
            // '#' absorbs everything the other filter has left, whatever it is.
            if (a[i].equals("#") || b[j].equals("#")) {
                return true;
            }
            if (a[i].equals("+") || b[j].equals("+") || a[i].equals(b[j])) {
                i++;
                j++;
                continue;
            }
            return false;
        }
    }
}
