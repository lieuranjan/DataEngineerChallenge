# DataEngineerChallenge

## Processing & Analytical goals:

Initialization of Spark Datasets
---
     val inputDataset:Dataset[LogSchema]=spark
      .read
      .option("delimiter"," ")
      .option("quote","\"")
      .option("header",false)
      .option("charset",StandardCharsets.UTF_8.name())
      .schema(schema)
      .csv(inputPath)
      .as(encoder)
      .repartition(numOfPartitions)
---

1. Sensitize the web log by IP. Sensitize = aggregate all page hits by visitor/IP during a session.
---
    val formattedDatasets=inputDataset
      .withColumn("client_host",split(inputDataset("client_host_port"),":").getItem(0))
        .withColumn("url",split(inputDataset("request")," ").getItem(1))
    //formattedDatasets.show(false)
    //removing extra not used columns
    var datasetForSessionize=formattedDatasets.select("create_time","client_host","url")
    datasetForSessionize=datasetForSessionize.persist(StorageLevels.MEMORY_AND_DISK_SER_2)

    //GET total hits from a host during an interval
    var sessionedData=datasetForSessionize.withColumn("interval", window(datasetForSessionize("create_time"), "15 minutes"))
    //persisting datasets as we will be using this multiple times.
    ///sessionedData=sessionedData.persist(StorageLevels.MEMORY_AND_DISK_SER_2)
    //sessionedData.show(false)
    val dsForTotalHits=sessionedData.groupBy(sessionedData("interval"),sessionedData("client_host")).count().as("num_hits_ip")
  ---
  

2. Determine the average session time
---

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times