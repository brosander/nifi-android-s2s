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

import android.app.job.JobInfo;
import android.app.job.JobParameters;
import android.app.job.JobService;
import android.content.ComponentName;
import android.content.Context;
import android.os.Build;
import android.os.Handler;
import android.os.PersistableBundle;
import android.support.annotation.RequiresApi;
import android.util.Log;

import com.hortonworks.hdf.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import com.hortonworks.hdf.android.sitetosite.client.persistence.SQLiteIOException;
import com.hortonworks.hdf.android.sitetosite.client.persistence.SiteToSiteDB;
import com.hortonworks.hdf.android.sitetosite.util.SerializationUtils;

import java.io.IOException;

@RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
public class SiteToSiteJobService extends JobService {

    public static final String CANONICAL_NAME = SiteToSiteJobService.class.getCanonicalName();

    @Override
    public boolean onStartJob(final JobParameters params) {
        final Context applicationContext = getApplicationContext();
        final SiteToSiteDB siteToSiteDB = new SiteToSiteDB(applicationContext);

        PersistableBundle extras = params.getExtras();
        final ParcelableQueuedOperationResultCallback parcelableQueuedOperationResultCallback = SerializationUtils.getParcelable(SiteToSiteJobService.class.getClassLoader(), extras, "callback");

        QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig = SerializationUtils.getParcelable(SiteToSiteJobService.class.getClassLoader(), extras, "config");
        try {
            siteToSiteDB.updatePeerStatusOnConfig(queuedSiteToSiteClientConfig);
        } catch (SQLiteIOException e) {
            parcelableQueuedOperationResultCallback.onException(applicationContext, e);
            jobFinished(params, true);
            return false;
        }

        SiteToSiteService.processQueuedPackets(applicationContext, queuedSiteToSiteClientConfig, new QueuedOperationResultCallback() {
            @Override
            public Handler getHandler() {
                return null;
            }

            @Override
            public void onSuccess() {
                if (parcelableQueuedOperationResultCallback != null) {
                    parcelableQueuedOperationResultCallback.onSuccess(applicationContext);
                }
                jobFinished(params, false);
            }

            @Override
            public void onException(IOException exception) {
                if (parcelableQueuedOperationResultCallback != null) {
                    parcelableQueuedOperationResultCallback.onException(applicationContext, exception);
                }
                jobFinished(params, true);
            }
        });
        return true;
    }

    @Override
    public boolean onStopJob(JobParameters params) {
        Log.w(CANONICAL_NAME, "Getting stopped by JobScheduler.");
        return true;
    }

    public static JobInfo.Builder createProcessJobInfoBuilder(Context context, int jobId, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, ParcelableQueuedOperationResultCallback parcelableQueuedOperationResultCallback) {
        JobInfo.Builder builder = new JobInfo.Builder(jobId, new ComponentName(context, SiteToSiteJobService.class));
        PersistableBundle persistableBundle = new PersistableBundle();
        SerializationUtils.putParcelable(queuedSiteToSiteClientConfig, persistableBundle, "config");
        SerializationUtils.putParcelable(parcelableQueuedOperationResultCallback, persistableBundle, "callback");
        builder.setExtras(persistableBundle);
        return builder;
    }
}
