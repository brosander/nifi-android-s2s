# Apache NiFi Site-to-Site Android library
#### A small (currently ~= 125k), easy-to-use Android library for sending data to NiFi via the Site-to-Site protocol with only Android SDK dependencies

### Building
```shell
export ANDROID_HOME=YOUR_SDK_DIR
./gradlew clean build
```

### Testing
To run the Android Test classes (on-device tests) along with the build start up an an Android Virtual Device and then run the following:
```shell
export ANDROID_HOME=YOUR_SDK_DIR
./gradlew clean connectedAndroidTest build
```

### Structure
* s2s: Android library
* demo: Demo app

### Usage
#### Setup
```java
// Need to be on right thread if updating UI, can return null handler in callback otherwise
final Handler handler = new Handler(Looper.getMainLooper());

SiteToSiteRemoteCluster siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();
siteToSiteRemoteCluster.setUrls(Arrays.asList("http://nifi.hostname:8080/nifi"));

SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
siteToSiteClientConfig.setRemoteClusters(Collections.singletonList(siteToSiteRemoteCluster));
siteToSiteClientConfig.setPortName("From Android");
```
#### Sample data
```java
Map<String, String> attributes = new HashMap<>();
attributes.put("key", "value");
DataPacket dataPacket = new ByteArrayDataPacket(attributes, "message".getBytes(Charsets.UTF_8));
```
#### One-shot

```java
// Synchronous
SiteToSiteClient siteToSiteClient = siteToSiteClientConfig.createClient();
Transaction transaction = siteToSiteClient.createTransaction();
transaction.send(dataPacket);
transaction.confirm();
TransactionResult transactionResult = transaction.complete();

// Asynchronous
SiteToSiteService.sendDataPacket(context, dataPacket, siteToSiteClientConfig, new TransactionResultCallback() {});
```

#### Repeating
This example schedules a repeating callback using the AlarmManager to dataCollector.getDataPackets() and sends the results to NiFi.  This repeating alarm will persist even when the app is terminated.

```java
// Can be any ParcelableTransactionResultCallback implementation
ParcelableTransactionResultCallback parcelableTransactionResultCallback = new RepeatingTransactionResultCallback();

// Can be any DataCollector implementation
DataCollector dataCollector = new TestDataCollector("message");
SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createSendPendingIntent(context, dataCollector, siteToSiteClientConfig, parcelableTransactionResultCallback);

AlarmManager alarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
alarmManager.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), TimeUnit.MINUTES.toMillis(15), siteToSiteRepeatableIntent.getPendingIntent());
```

#### Queued Processing
In many cases, it can be useful to queue up data to send to NiFi and then wait until more favorable conditions to send the data.

This can be easily accomplished by Queuing data using the SiteToSiteService and/or the SiteToSiteRepeatingService and then using the JobScheduler to send the data when the desired criteria is met.
##### Setup
```java
SiteToSiteRemoteCluster siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();
siteToSiteRemoteCluster.setUrls(Arrays.asList("http://nifi.hostname:8080/nifi"));

QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig = new QueuedSiteToSiteClientConfig();
queuedSiteToSiteClientConfig.setRemoteClusters(Collections.singletonList(siteToSiteRemoteCluster));
queuedSiteToSiteClientConfig.setPortName("From Android");

// Set the maximum amount of time before a transaction is considered failed in case it failed but was unable to update database to indicate that.  This should be set much higher than transactions will be reasonably expected to take and defaults to 10 minutes.
queuedSiteToSiteClientConfig.setMaxTransactionTime(20, TimeUnit.MINUTES);

// Optionally set a data packet prioritizer.  This is responsible for assigning a numeric priority to each packet (higher is more important) as well as a ttl value beyond which the packet should be discarded whether sent or not.
queuedSiteToSiteClientConfig.setDataPacketPrioritizer(new YourCustomDataPacketPrioritizer());

// Optionally set the maximum number of rows to retain when cleanup is performed
queuedSiteToSiteClientConfig.setMaxRows(10000);

// Optionally set the maximum size to use storing the attributes and flowfile content in the database (more expensive to age-off than row count or ttl)
queuedSiteToSiteClientConfig.setMaxSize(1024 * 100);
```

##### One-shot

###### Enqueue
```java
// Synchronous
QueuedSiteToSiteClient queuedSiteToSiteClient = queuedSiteToSiteClientConfig.getQueuedSiteToSiteClient(context);
queuedSiteToSiteClient.enqueue(dataPacket);

// Asynchronous
SiteToSiteService.enqueueDataPacket(context, dataPacket, queuedSiteToSiteClientConfig, new QueuedOperationResultCallback(){});
SiteToSiteService.enqueueDataPackets(context, dataPackets, queuedSiteToSiteClientConfig, new QueuedOperationResultCallback(){});

// Repeating
SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createEnqueuePendingIntent(context, dataCollector, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
AlarmManager alarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
alarmManager.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), TimeUnit.MINUTES.toMillis(15), siteToSiteRepeatableIntent.getPendingIntent());
```

###### Process
```java
// Synchronous
queuedSiteToSiteClient.process();

// Asynchronous
SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, new QueuedOperationResultCallback(){});

// Repeating
SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createProcessQueuePendingIntent(context, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
AlarmManager alarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
alarmManager.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), TimeUnit.MINUTES.toMillis(15), siteToSiteRepeatableIntent.getPendingIntent());

// Criteria-based
JobInfo.Builder processJobInfoBuilder = SiteToSiteJobService.createProcessJobInfoBuilder(context, jobId, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
processJobInfoBuilder.setRequiredNetworkType(JobInfo.NETWORK_TYPE_UNMETERED);
processJobInfoBuilder.setRequiresCharging(true);
JobScheduler jobScheduler = (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
```

###### Cleanup
```java
// Synchronous
queuedSiteToSiteClient.cleanup();

// Asynchronous
SiteToSiteService.cleanupQueuedPackets(context, queuedSiteToSiteClientConfig, new QueuedOperationResultCallback(){});

// Repeating
SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createCleanupQueuePendingIntent(context, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
AlarmManager alarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
alarmManager.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), TimeUnit.MINUTES.toMillis(15), siteToSiteRepeatableIntent.getPendingIntent());
```