package com.jinternals.mqtt.edge.protocol;

/**
 * The topic scheme, as this gateway uses it.
 *
 * <pre>
 *   sites/&lt;site&gt;/telemetry/&lt;robot&gt;        edge → cloud
 *   sites/&lt;site&gt;/health/&lt;robot&gt;           edge → cloud, retained (+ Last Will)
 *   sites/&lt;site&gt;/command/&lt;robot&gt;/req      cloud → edge
 *   sites/&lt;site&gt;/command/&lt;robot&gt;/res      edge → cloud
 *   sites/&lt;site&gt;/bridge/state              broker notification, retained
 * </pre>
 *
 * <h2>Why the site comes first</h2>
 *
 * The site is the <b>tenant boundary</b> of this system — it is the unit that gets a credential, an
 * ACL, a bridge and an uplink that fails independently of every other site. Putting it at the root
 * makes that boundary a single prefix, so:
 *
 * <ul>
 *   <li>"this operator may read everything at site 1, and nothing anywhere else" is one ACL line,
 *       {@code topic read sites/site1/#}, rather than one line per message kind that must be
 *       revisited every time a kind is added;
 *   <li>a topic browser shows one branch per site with that site's telemetry, health and commands
 *       underneath it — which is how you actually look at a fleet, rather than having each site's
 *       data scattered across a {@code telemetry/} branch, a {@code health/} branch and a
 *       {@code command/} branch;
 *   <li>a new message kind ({@code alarm}, {@code config}) slots in under a site without touching
 *       the shape of anything above it.
 * </ul>
 *
 * <p>The cost is that fleet-wide, single-kind subscriptions gain a wildcard —
 * {@code sites/+/telemetry/+} rather than {@code telemetry/+/+}. That is a fair trade: cross-site
 * reads are a handful of subscriptions in one service, whereas the tenant boundary is enforced on
 * every broker, in every ACL, for every site. Optimise the layout for the thing there is more of.
 *
 * <p>(This is also how Sparkplug B orders its namespace — group id before message type — for the
 * same reason.)
 *
 * <p>The {@code sites/} root exists so that genuinely fleet-wide topics have somewhere to live that
 * is not inside any one tenant's subtree.
 *
 * <p>Deliberately duplicated in the cloud service rather than shared through a common jar: the two
 * deploy on schedules that have nothing to do with each other, and a shared artifact turns every
 * cloud release into a fleet-wide gateway release. The contract of record is the scheme above, not
 * a Java type.
 */
public final class Topics {

    public static final String ROOT = "sites";

    private Topics() {}

    /** {@code sites/<site>/telemetry/<robot>} — published by this gateway. */
    public static String telemetry(String site, String robotId) {
        return ROOT + "/" + site + "/telemetry/" + robotId;
    }

    /** {@code sites/<site>/health/<robot>} — published retained; also the Last Will topic. */
    public static String health(String site, String robotId) {
        return ROOT + "/" + site + "/health/" + robotId;
    }

    /** {@code sites/<site>/command/+/req} — everything addressed to this site, and nothing else. */
    public static String commandRequestFilter(String site) {
        return ROOT + "/" + site + "/command/+/req";
    }

    /** {@code sites/<site>/command/<robot>/res} — this gateway's answers. */
    public static String commandResponse(String site, String robotId) {
        return ROOT + "/" + site + "/command/" + robotId + "/res";
    }

    /**
     * Pulls the site and robot out of an inbound command topic
     * ({@code sites/<site>/command/<robot>/req}). Returns {@code null} for anything that does not
     * fit the scheme, so callers can log-and-drop rather than throw on the MQTT thread.
     */
    public static SiteRobot parseCommandTopic(String topic) {
        String[] p = topic.split("/");
        if (p.length != 5 || !ROOT.equals(p[0]) || !"command".equals(p[2]) || !"req".equals(p[4])) {
            return null;
        }
        return new SiteRobot(p[1], p[3]);
    }

    public record SiteRobot(String site, String robotId) {}
}
