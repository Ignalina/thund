# Thund , DAG processor based on Apache Arrow.
A modern and performant/robust dag processor for data pipelines allowing processing data in/out from storages like S3 or Iceberg/delta lakes without interruptions.

# Why ??
Whenever it is not feasible for an Apache Airflow / NIFI / Hadoop flying circus alike. Legacy software could remain operating on your storage/lake data in conjunction with Thund handling In/Out.  For a complete modern stack combine Apache Arrows Balista/Datafusion in combination with Thund.   


If you dont get it , no worries its an early experiment , perhaps "Grímnismál" (Year 1300-1325) in the Poetic Edda explains it goal better  
*__"Thunda's waters hast'ning fleet,__*  
*__Touch not Valgom! with thy feet."__*


# Design goals are
* Apache Arrow centric
* Minimalistic / Performant / Robust
* Conform to  Brokkr's Software critera https://github.com/Ignalina/brokkr


Goals below are to be sorted for V1,V2 or V never

### Functional Goals  V0
* Fix eventhandler/step arguments from simple reader to functions for create reader and writer.
* Picture of the watcher->eventhandlers mechanics and tossing of parameters.

### Functional Goals  V1 
* Alloy component , Could Arrow references be used betwen Golang-Rust ?
* Support for Arrows filesystem HDFS,
* Incorporate RCLONE
* Graph support
* Add handlers to Arrow->Tantivy/Apache flight/kafka/delta-rs
* Handlers Deployable/Callable from minifi

### Functional Goals V2

* Steps spread out on multiple Processors
* Jaeger 
* Metrics
* Static Deployment via ipmi
* Deployment via kubernetes, as static as possible.


### Thund in the litterature
Translations poeems describing Thund [Germanic mythology](http://www.germanicmythology.com/PoeticEdda/GRM21.html)  
Learn pronounce in Icelandic [ÓÐSMÁL](https://odsmal.org/thund-thund-mythological-river)  


