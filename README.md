# FineGrainedDataProvenance

Repository for the [FlowDebug paper](https://jiateoh.github.io/pdfs/FlowDebug_SoCC2020_camera-ready.pdf).

Some quick notes on building:
* The current codebase contains code for running another provenance debugging tool, BigSift. If you do not need to run BigSift, you can exclude or remove the corresponding files/packages (src/main/examples/benchmarks/bigsift*). 
* The repository depends on spark and spark-sql. You can either provide your own (e.g. create a lib/ folder and place spark jars inside) or modify the build.sbt file to include those dependencies (which are currently commented out).
  * If using BigSift, you can add the bigsift spark build here instead.
