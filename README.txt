This is not going to be particularly useful to anyone outside of local.ch ( http://www.local.ch/ )

Background info:
We're the swiss Yellow Pages and as such have a pretty popular website. 
Various parts of our infrastructure generate logs.
Those logs are using formats that only make sense to us, so don't expect your average apache common log file.

The fact we have several logs families, with the same family having gone through several iterations, explains the general structure of the project.

The different files in the project as designed as "Lego bricks" which you assemble to design a new log processing pipeline.

Things could arguably be made much simpler by using tighter integration between the different components, but our pipelines have a twist:
We use EC2 for parallel processing on smallish instances, and store intermediate results for forklift updates into a large database instance.
The large instance uses the Elastic Block Storage for persistence, and is only started when
* we have data worth loading.
* we need to run reports.