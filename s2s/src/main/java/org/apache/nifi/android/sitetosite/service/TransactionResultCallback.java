/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.android.sitetosite.service;

import android.os.Handler;
import android.os.Parcelable;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;

import java.io.IOException;

/**
 * Callback to be invoked on transaction result
 */
public interface TransactionResultCallback {
    /**
     * Handler to do the invoking
     *
     * @return the handler
     */
    Handler getHandler();

    /**
     * Success callback
     *
     * @param transactionResult the result
     * @param siteToSiteClientConfig (possibly updated) s2s config
     */
    void onSuccess(TransactionResult transactionResult, SiteToSiteClientConfig siteToSiteClientConfig);

    /**
     * Failure callback
     * @param exception the error
     * @param siteToSiteClientConfig (possibly updated) s2s config
     */
    void onException(IOException exception, SiteToSiteClientConfig siteToSiteClientConfig);
}
