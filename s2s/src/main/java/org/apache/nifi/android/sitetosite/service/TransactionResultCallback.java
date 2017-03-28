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

import android.os.Bundle;
import android.os.Handler;
import android.os.ResultReceiver;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;

import java.io.IOException;

import static org.apache.nifi.android.sitetosite.service.SiteToSiteService.IO_EXCEPTION;
import static org.apache.nifi.android.sitetosite.service.SiteToSiteService.RESULT;

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

    /**
     * Class to proxy the callback via a ResultReceiver (a must for the IntentService)
     */
    final class Receiver extends ResultReceiver {
        private final TransactionResultCallback delegate;

        /**
         * Create a new ResultReceive to receive results.  Your
         * {@link #onReceiveResult} method will be called from the thread running
         * <var>handler</var> if given, or from an arbitrary thread if null.
         *
         * @param delegate
         */
        public Receiver(TransactionResultCallback delegate) {
            super(delegate.getHandler());
            this.delegate = delegate;
        }

        @Override
        protected void onReceiveResult(int resultCode, Bundle resultData) {
            super.onReceiveResult(resultCode, resultData);
            SiteToSiteClientConfig siteToSiteClientConfig = resultData.getParcelable(SiteToSiteService.SITE_TO_SITE_CONFIG);
            TransactionResult transactionResult = resultData.getParcelable(RESULT);

            if (resultCode == 0) {
                delegate.onSuccess(transactionResult, siteToSiteClientConfig);
            } else {
                delegate.onException((IOException) resultData.getSerializable(IO_EXCEPTION), siteToSiteClientConfig);
            }
        }

        public static ResultReceiver wrap(TransactionResultCallback transactionResultCallback) {
            return new Receiver(transactionResultCallback);
        }

        /**
         * Sends the given result to the receiver
         *
         * @param resultReceiver         the receiver
         * @param transactionResult      the result
         * @param siteToSiteClientConfig (possibly updated) s2s config
         */
        public static void onSuccess(ResultReceiver resultReceiver, TransactionResult transactionResult, SiteToSiteClientConfig siteToSiteClientConfig) {
            Bundle resultData = new Bundle();
            resultData.putParcelable(SiteToSiteService.SITE_TO_SITE_CONFIG, siteToSiteClientConfig);
            resultData.putParcelable(RESULT, transactionResult);
            resultReceiver.send(0, resultData);
        }

        /**
         * Sends the given exception to the receiver
         *
         * @param resultReceiver         the receiver
         * @param exception              the exception
         * @param siteToSiteClientConfig (possibly updated) s2s config
         */
        public static void onException(ResultReceiver resultReceiver, IOException exception, SiteToSiteClientConfig siteToSiteClientConfig) {
            Bundle resultData = new Bundle();
            resultData.putParcelable(SiteToSiteService.SITE_TO_SITE_CONFIG, siteToSiteClientConfig);
            resultData.putSerializable(IO_EXCEPTION, exception);
            resultReceiver.send(1, resultData);
        }
    }
}
