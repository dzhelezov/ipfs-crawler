# ipfs-crawler

This small tool sends random key queries to the IPFS DHT, which in turns triggers connections to 
random nodes in the network. The queries and the spawned connections are then logged, and one then can estimate
the network size and the churn. 

By averaging the maximal common length prefix of peers ID with random key shots, one can ballpark (up to say 2x) the network size. Counting the connections gives a more accurate estimate but it takes around 20-30 minutes to saturate. 

Some basic R data analysis is present in `logs-analysis.R`

Kudos to [@raulk](https://github.com/raulk/) for the original version of the [hawk](https://github.com/raulk/dht-hawk)

## License

Authored by raulk. Dual-licensed under MIT and ASLv2, by way of the [Permissive License Stack](https://protocol.ai/blog/announcing-the-permissive-license-stack/).
