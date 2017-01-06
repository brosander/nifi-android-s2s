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

package org.apache.nifi.android.sitetosite.client.persistence;

import org.apache.nifi.android.sitetosite.client.TransactionResult;

import java.io.IOException;
import java.util.Date;

public class TransactionLogEntry {
    private long id;
    private final Date created;
    private final TransactionResult transactionResult;
    private final IOException ioException;

    public TransactionLogEntry(TransactionResult transactionResult) {
        this(-1, new Date(), transactionResult, null);
    }

    public TransactionLogEntry(IOException ioException) {
        this(-1, new Date(), null, ioException);
    }

    public TransactionLogEntry(long id, Date created, TransactionResult transactionResult, IOException ioException) {
        this.id = id;
        this.created = created;
        this.transactionResult = transactionResult;
        this.ioException = ioException;
    }

    public long getId() {
        return id;
    }

    public Date getCreated() {
        return created;
    }

    protected void setId(long id) {
        this.id = id;
    }

    public TransactionResult getTransactionResult() {
        return transactionResult;
    }

    public IOException getIoException() {
        return ioException;
    }
}
