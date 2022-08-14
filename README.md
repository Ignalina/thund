# Thund , modern processor based on Apache Arrow. 

# Why ??
Whenever it is not feasible for an Apache Airflow / NIFI alike . 
Instead Thund's aproach are modern and performant/robust processor/pipe allowing data in/out from storages like S3 or Iceberg/delta lakes without interruptions. Keep your NIFI/Airflow or Hadoop flying circus or even better throw them out and use Apache Arrows Balista/Datafusion in combination with Thund. 


If dont get it , no worries its an early experiment , perhaps "Grímnismál" in the Poetic Edda explains it goal better  
*__"Thunda's waters hast'ning fleet,__*  
*__Touch not Valgom! with thy feet."__*


### Design goals are
* Apache Arrow centric
* Minimalistic / Performant / Robust
* Conform to  Brokkr's Software critera https://github.com/Ignalina/brokkr


Goals below are to be sorted for V1,V2 or V never

### Functional Goals  V1 
* Alloy component , Could Arrow references be used betwen Golang-Rust ?
* Support for Arrows filesystem HDFS,
* Incorporate RCLONE
* Graph support
* Callable from minifi

### Functional Goals V2

* Steps spread out on multiple Processors
* Jaeger 
* Metrics
* Static Deployment via ipmi
* Deployment via kubernetes, as static as possible.

### The name Thund


### Thund in the litterature
Translations poeems describing Thund [Germanic mytholigy](http://www.germanicmythology.com/PoeticEdda/GRM21.html)  
Learn pronounce in Icelandic [ÓÐSMÁL](https://odsmal.org/thund-thund-mythological-river)  


