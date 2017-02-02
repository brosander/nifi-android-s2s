package org.apache.nifi.android.sitetosite.util;

import okhttp3.mockwebserver.RecordedRequest;

public interface RequestAssertion {
    RecordedRequest check() throws Exception;
}
