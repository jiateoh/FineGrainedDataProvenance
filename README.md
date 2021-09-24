# FlowDebug
Influence-Based Provenance for Dataflow Applications with Taint Propagation (SoCC 2020)

## Summary of FlowDebug
Debugging big data analytics often requires a root cause analysis to pinpoint the precise culprit records in an input dataset responsible for incorrect or anomalous output. Existing debugging or data provenance approaches do not track fine-grained control and data flows in user-defined application code; thus, the returned culprit data is often too large for manual inspection and expensive post-mortem analysis is required.
We design FlowDebug to identify a highly precise set of input records based on two key insights. First, FlowDebug precisely tracks control and data flow within user-defined functions to propagate taints at a fine-grained level by inserting custom data abstractions through automated source to source transformation. Second, it introduces a novel notion of influence-based provenance for many-to-one dependencies to prioritize which input records are more responsible than others by analyzing the semantics of a user-defined function used for aggregation. By design, our approach does not require any modification to the framework’s runtime and can be applied to existing applications easily. FlowDebug significantly improves the precision of debugging results by up to 99.9 percentage points and avoids repetitive re-runs required for post-mortem analysis by a factor of 33 while incurring an instrumentation overhead of 0.4X - 6.1X on vanilla Spark

## Team
This project is developed by Professor [Miryung Kim](http://web.cs.ucla.edu/~miryung/)'s Software Engineering and Analysis Laboratory at UCLA. 
If you encounter any problems, please open an issue or feel free to contact us:

[Jason Teoh](http://https://jiateoh.github.io/): PhD student, jteoh@cs.ucla.edu;

[Muhammad Ali Gulzar](https://people.cs.vt.edu/~gulzar/): Professor at Virginia Tech, gulzar@cs.vt.edu;

[Miryung Kim](http://web.cs.ucla.edu/~miryung/): Professor at UCLA, miryung@cs.ucla.edu;

## How to cite 
Please refer to our SoCC 2020 paper, [Influence-Based Provenance for Dataflow Applications with Taint Propagation](http://web.cs.ucla.edu/~miryung/Publications/socc2020-flowdebug.pdf) for more details. 

### Bibtex  
@inproceedings{10.1145/3419111.3421292,
author = {Teoh, Jason and Gulzar, Muhammad Ali and Kim, Miryung},
title = {Influence-Based Provenance for Dataflow Applications with Taint Propagation},
year = {2020},
isbn = {9781450381376},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3419111.3421292},
doi = {10.1145/3419111.3421292},
abstract = {Debugging big data analytics often requires a root cause analysis to pinpoint the
precise culprit records in an input dataset responsible for incorrect or anomalous
output. Existing debugging or data provenance approaches do not track fine-grained
control and data flows in user-defined application code; thus, the returned culprit
data is often too large for manual inspection and expensive post-mortem analysis is
required.We design FlowDebug to identify a highly precise set of input records based
on two key insights. First, FlowDebug precisely tracks control and data flow within
user-defined functions to propagate taints at a fine-grained level by inserting custom
data abstractions through automated source to source transformation. Second, it introduces
a novel notion of influence-based provenance for many-to-one dependencies to prioritize
which input records are more responsible than others by analyzing the semantics of
a user-defined function used for aggregation. By design, our approach does not require
any modification to the framework's runtime and can be applied to existing applications
easily. FlowDebug significantly improves the precision of debugging results by up
to 99.9 percentage points and avoids repetitive re-runs required for post-mortem analysis
by a factor of 33 while incurring an instrumentation overhead of 0.4X - 6.1X on vanilla
Spark.},
booktitle = {Proceedings of the 11th ACM Symposium on Cloud Computing},
pages = {372–386},
numpages = {15},
keywords = {data intensive scalable computing, fault localization, big data systems, data provenance, taint analysis},
location = {Virtual Event, USA},
series = {SoCC '20}
}

[DOI Link](https://dl.acm.org/doi/10.1145/3419111.3421292)

## Build Notes:
* The current codebase contains code for running another provenance debugging tool, BigSift. If you do not need to run BigSift, you can exclude or remove the corresponding files/packages (src/main/examples/benchmarks/bigsift*). 
* The repository depends on spark and spark-sql. You can either provide your own (e.g. create a lib/ folder and place spark jars inside) or modify the build.sbt file to include those dependencies (which are currently commented out). Note that RoaringBitmap is explicitly included in `build.sbt`, so be sure to remove that dependency if manually including jars.
  * If using BigSift, you can add the bigsift spark build here instead.





