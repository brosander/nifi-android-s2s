package org.apache.nifi.android.sitetosite.client.protocol;

public class Headers {
    public static final String ACCEPT = "Accept";
    public static final String AUTHORIZATION = "Authorization";

    public static final String CONTENT_TYPE = "Content-Type";

    public static final String HANDSHAKE_PROPERTY_BATCH_COUNT = "x-nifi-site-to-site-batch-count";
    public static final String HANDSHAKE_PROPERTY_BATCH_SIZE = "x-nifi-site-to-site-batch-size";
    public static final String HANDSHAKE_PROPERTY_BATCH_DURATION = "x-nifi-site-to-site-batch-duration";
    public static final String HANDSHAKE_PROPERTY_REQUEST_EXPIRATION = "x-nifi-site-to-site-request-expiration";
    public static final String HANDSHAKE_PROPERTY_USE_COMPRESSION = "x-nifi-site-to-site-use-compression";

    public static final String LOCATION_HEADER_NAME = "Location";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "transaction-url";

    public static final String PROTOCOL_VERSION = "x-nifi-site-to-site-protocol-version";
    public static final String PROXY_AUTHORIZATION = "Proxy-Authorization";

    public static final String SERVER_SIDE_TRANSACTION_TTL = "x-nifi-site-to-site-server-transaction-ttl";
}
