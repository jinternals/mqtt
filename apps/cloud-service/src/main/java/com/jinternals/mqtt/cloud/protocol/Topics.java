package com.jinternals.mqtt.cloud.protocol;

/**
 * The topic scheme, as the cloud service uses it.
 *
 * <pre>
 *   sites/&lt;site&gt;/telemetry/&lt;robot&gt;        edge → cloud
 *   sites/&lt;site&gt;/health/&lt;robot&gt;           edge → cloud, retained (+ Last Will)
 *   sites/&lt;site&gt;/command/&lt;robot&gt;/req      cloud → edge
 *   sites/&lt;site&gt;/command/&lt;robot&gt;/res      edge → cloud
 *   sites/&lt;site&gt;/bridge/state              broker notification, retained
 * </pre>
 *
 * <p>Site-first, because the site is the tenant boundary: it is what gets a credential, an ACL, a
 * bridge and an uplink that fails on its own. As a root prefix that boundary is one ACL line per
 * site instead of one per message kind, and a topic browser shows one branch per site rather than
 * scattering each site across a {@code telemetry/}, a {@code health/} and a {@code command/}
 * branch.
 *
 * <p>The cost lands here, and it is small: fleet-wide subscriptions gain a wildcard
 * ({@code sites/+/telemetry/+}). Four subscriptions in one service, versus a tenant boundary
 * enforced on every broker for every site — the right thing to make cheap is the one there is more
 * of.
 *
 * <p>Deliberately duplicated rather than shared with the edge service; see the note in that copy.
 */
public final class Topics {

    public static final String ROOT = "sites";

    private Topics() {}

    /** {@code sites/<site>/command/<robot>/req} — the only topic this service publishes to. */
    public static String commandRequest(String site, String robotId) {
        return ROOT + "/" + site + "/command/" + robotId + "/req";
    }

    // --- fleet-wide subscriptions ---------------------------------------------------------

    public static final String ALL_TELEMETRY = ROOT + "/+/telemetry/+";
    public static final String ALL_HEALTH = ROOT + "/+/health/+";
    public static final String ALL_COMMAND_RESPONSES = ROOT + "/+/command/+/res";

    /** Mosquitto publishes each site's bridge up/down here (see {@code notification_topic}). */
    public static final String ALL_BRIDGE_STATE = ROOT + "/+/bridge/state";

    /** Extracts the site from any {@code sites/<site>/...} topic. */
    public static String siteOf(String topic) {
        String[] p = topic.split("/");
        return p.length >= 2 && ROOT.equals(p[0]) ? p[1] : null;
    }
}
