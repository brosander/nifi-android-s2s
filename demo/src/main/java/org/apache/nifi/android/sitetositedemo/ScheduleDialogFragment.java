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

package org.apache.nifi.android.sitetositedemo;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.app.Dialog;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.v4.app.DialogFragment;
import android.support.v7.app.AlertDialog;
import android.view.View;
import android.widget.EditText;
import android.widget.Spinner;

public class ScheduleDialogFragment extends DialogFragment {
    private ScheduleDialogCallback scheduleDialogCallback;

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
        // Lint warning not relevant for alert dialogs
        @SuppressLint("InflateParams") final View view = getActivity().getLayoutInflater().inflate(R.layout.dialog_schedule, null);
        view.post(new Runnable() {
            @Override
            public void run() {
                ((Spinner)view.findViewById(R.id.alarm_interval_units_spinner)).setSelection(1);
            }
        });
        builder.setView(view)
                .setPositiveButton(R.string.schedule_confirm, new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        String interval = ((EditText)view.findViewById(R.id.alarm_interval_text)).getText().toString();
                        ScheduleDialogCallback.TimeUnits timeUnits = ScheduleDialogCallback.TimeUnits.values()[((Spinner)view.findViewById(R.id.alarm_interval_units_spinner)).getSelectedItemPosition()];
                        scheduleDialogCallback.onConfirm(timeUnits.convertToMillis(Long.parseLong(interval)));
                    }
                })
                .setNegativeButton(R.string.schedule_cancel, new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {

                    }
                });
        return builder.create();
    }

    @Override
    public void onAttach(Activity activity) {
        super.onAttach(activity);
        scheduleDialogCallback = (ScheduleDialogCallback) activity;
    }
}
