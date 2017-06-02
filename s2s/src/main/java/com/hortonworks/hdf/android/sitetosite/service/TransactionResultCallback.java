/*
 * Copyright 2017 Hortonworks, Inc.
 * All rights reserved.
 *
 *   Hortonworks, Inc. licenses this file to you under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License. You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * See the associated NOTICE file for additional information regarding copyright ownership.
 */

package com.hortonworks.hdf.android.sitetosite.service;

import android.os.Bundle;
import android.os.Handler;
import android.os.ResultReceiver;

import com.hortonworks.hdf.android.sitetosite.client.TransactionResult;

import java.io.IOException;

import static com.hortonworks.hdf.android.sitetosite.service.SiteToSiteService.IO_EXCEPTION;
import static com.hortonworks.hdf.android.sitetosite.service.SiteToSiteService.RESULT;

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
     */
    void onSuccess(TransactionResult transactionResult);

    /**
     * Failure callback
     * @param exception the error
     */
    void onException(IOException exception);

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
            TransactionResult transactionResult = resultData.getParcelable(RESULT);

            if (resultCode == 0) {
                delegate.onSuccess(transactionResult);
            } else {
                delegate.onException((IOException) resultData.getSerializable(IO_EXCEPTION));
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
         */
        public static void onSuccess(ResultReceiver resultReceiver, TransactionResult transactionResult) {
            if (resultReceiver == null) {
                return;
            }
            Bundle resultData = new Bundle();
            resultData.putParcelable(RESULT, transactionResult);
            resultReceiver.send(0, resultData);
        }

        /**
         * Sends the given exception to the receiver
         *
         * @param resultReceiver         the receiver
         * @param exception              the exception
         */
        public static void onException(ResultReceiver resultReceiver, IOException exception) {
            if (resultReceiver == null) {
                return;
            }
            Bundle resultData = new Bundle();
            resultData.putSerializable(IO_EXCEPTION, exception);
            resultReceiver.send(1, resultData);
        }
    }
}
