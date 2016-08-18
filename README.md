# pprl-scaling-framework

## A scalable Privacy-Preserving Record Linkage (PPRL) framework for large datasets on the Apache Hadoop ecosystem.


The Record Linkage (RL) process tries to find pairs of entities across different databases that refer to the 
same real-wold object.This process is increasingly used in research applications for example in medicine, 
social studies and official statistics. In these fields, protecting the identifiers of the entities is usually
required by law. Therefore, if such linkages are permitted, special techniques protecting the identifiers have
to be used. The set of techniques for record linkage without revealing identifiers is called Privacy Preserving
 Record Linkage (PPRL).

Like traditional approaches for record linkage, PPRL has an inherent scalability problem if each encrypted record
needs to be compared with each other record resulting in a quadratic complexity. The usual means to improve efficiency 
and thus scalability to larger datasets is to reduce the search space, e.g. by appropriate filter and blocking techniques,
or/and to perform record linkage related tasks in parallel on many processors.

The aim of this project is to develop a basic framework (tools and libraries) that provides the necessary functionality
for multiple parties to perform PPRL between their datasets on the hadoop cluster (pprl-site). Furthermore, since the 
whole process takes place in a shared cluster environment this framework must provide highly secure semantics 
so data custodians feel safe in uploading sensitive data in the pprl cluster.
