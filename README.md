## Anomaly detection via the use of unsupervised learning on a spark cluster

### Introduction
I have a data set that contains a week-long stream of compiled log security data from a network that reported an intrusion.  My initial goal was just to find this network oddity.  In doing so I have started to engineer features from the data set that can computed on the fly as data streams into the system.  

My goal now is to use this data to build a streaming service for anomaly detection mostly based on deviations of typical user behavior or typical network behavior in general.  I want this service to work in realtime and to send flags alerting the SOC that something odd is occuring on the network.  The data is unlabeled and thus typical network behavior must be determined using unsupervised algorithms and implimented using spark on a distributed cluster.  Eventually I want to incorporate this into a full SMACK-ish steaming service.



### Results:  


### packages to install:  
1. 

### To do  
1. Look at data space and determine what features could be engineered to focus the data space toward variables with more impactful information regarding 

### References:  
1. 
