# Apache NiFi Site-to-Site Android library
#### A small (currently ~= 85k), easy-to-use Android library for sending data to NiFi via the HTTP(S) Site-to-Site protocol with only Android SDK dependencies

### Building
```shell
./gradlew clean build
```

### Structure
* s2s: Android library
* demo: Demo app

### Usage
#### One-shot
```java
// Need to be on right thread if updating UI, can return null handler in callback otherwise
final Handler handler = new Handler(Looper.getMainLooper());

SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
siteToSiteClientConfig.setUrls(Arrays.asList("http://nifi.hostname:8080/nifi"));
siteToSiteClientConfig.setPortName("From Android");

Map<String, String> attributes = new HashMap<>();
attributes.put("key", "value");
DataPacket dataPacket = new ByteArrayDataPacket(attributes, "message".getBytes(Charsets.UTF_8));

SiteToSiteService.sendDataPacket(context, dataPacket, siteToSiteClientConfig, new TransactionResultCallback() {
  @Override
  public Handler getHandler() {
    return handler;
  }

  @Override
  public void onSuccess(TransactionResult transactionResult, SiteToSiteClientConfig siteToSiteClientConfig) {
    // Handle successful transaction
  }

  @Override
  public void onException(IOException exception, SiteToSiteClientConfig siteToSiteClientConfig) {
    // Handle exception
  }
});
```

#### Repeating
This example schedules a repeating callback using the AlarmManager to dataCollector.getDataPackets() and sends the results to NiFi.  This repeating alarm will persist even when the app is terminated.

```java
SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
siteToSiteClientConfig.setUrls(Arrays.asList("http://nifi.hostname:8080/nifi"));
siteToSiteClientConfig.setPortName("From Android");

// Can be any ParcelableTransactionResultCallback implementation
ParcelableTransactionResultCallback parcelableTransactionResultCallback = new RepeatingTransactionResultCallback();

// Can be any DataCollector implementation
DataCollector dataCollector = new TestDataCollector("message");
SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createPendingIntent(context, dataCollector, siteToSiteClientConfig, parcelableTransactionResultCallback);

AlarmManager alarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
alarmManager.setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), TimeUnit.MINUTES.toMillis(15), siteToSiteRepeatableIntent.getPendingIntent());
```
