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

import android.app.job.JobParameters;
import android.app.job.JobService;
import android.content.Context;
import android.os.Build;
import android.os.Handler;
import android.support.annotation.RequiresApi;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.util.SerializationUtils;

import java.io.IOException;

@RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
public class SiteToSiteJobService extends JobService {
    @Override
    public boolean onStartJob(final JobParameters params) {
        final Context applicationContext = getApplicationContext();
        final SiteToSiteDB siteToSiteDB = new SiteToSiteDB(applicationContext);

        QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig = SerializationUtils.getParcelable(SiteToSiteJobService.class.getClassLoader(), params.getExtras(), "config");
        siteToSiteDB.updatePeerStatusOnConfig(queuedSiteToSiteClientConfig);

        SiteToSiteService.processQueuedPackets(applicationContext, queuedSiteToSiteClientConfig, new QueuedOperationResultCallback() {
            @Override
            public Handler getHandler() {
                return null;
            }

            @Override
            public void onSuccess() {
                jobFinished(params, false);
            }

            @Override
            public void onException(IOException exception) {
                jobFinished(params, true);
            }
        });
        return true;
    }

    @Override
    public boolean onStopJob(JobParameters params) {
        return true;
    }
}
