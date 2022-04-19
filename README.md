
## Session Analysis

### Geospatial and Temporal Analysis of taxi traffic data in the city of New York     

	- aiming to improve each taxi's utilization rate
	- meet the requirement of being at the right time at the dispach center, on shift change, 
	  despite the traffic happening at specific times in a region daily, or other planned
	  factors that obstruct normal circulation of vehicles, for effective trip management by 
	  taxi drivers and vehicles' management by taxi vendors
	  
	There are many useful applications one can think of in this use case given the povided data.
	Here, taking inspiration by [8], the relation between the dropoff point and the duration in minutes
	that passes from the last dropoff of a passenger, until the pickup of the next client, is investigated.
	This serves as a good indicator for the utilization statistic, which is usefull for the taxi business. 	
 
 
 
Techniques followed

	Data preprocessing for cleaning the data set, with the taxi pickup and dropoff points, pickup and dropoff 
	datetimes and driver ids of one month for a city. Geospatial and time processing of the data in combination
	with a second data set, containing purely geographical information. Computation of the time duration between
	the last and the next fare and the next pickup borough. The last step of the analysis involves the presenta-
	tion of the results on a heatmap diagram.


  
Data set

	The data set used includes records of information on taxi trips for January 2013 [1,3,4]. This is only part
	of a much bigger data set including records on taxi trips and fare tariffs for all over the year and for 
	more than one years, as the effort of monitoring, collecting and publicing taxi traffic data in NYC gets 
	systematized. 



Challenges

	In this use case, we aim to quantify the factors that affect the utilization time of each taxi. Towards
	this purpose, we are going to compute the average time for a taxi to find its next fare as a function of
	the borough in which it dropped its passengers off [3]. For this computation, geospatial and temporal 
	data procesing is going to take place. This involves processing dates, times, longitude/latitude and
	geospatial boundaries, analyzing vectors at scale (working with polygons, lines, points etc) and others.
	As this information, coming from two seperate domains in commercial applications, time and space, has 
	its own intricacies, appropriate libraries need to be developed and used in place to simplify information
	management and the management of complexities innate to space and time domain, such the diversity in 
	various, formats, diversity of systems of reference (coordinate systems, calendars etc), reliability of
	information (accuracy of geographical information) and others. Hopefully, a wide range of libraries has 
	been developed for this purpose. Here [2] is a quite informed report on some of the challenges faced in 
	geospatial data processing and on some usefull libraries and tools that have been developed in this field. 
	In this case study Apache-Sedona [5] and Geospark [6] have been used for data processing and visualization 
	and PySpark [7] for the time and data overall analysis.
	
	The data set with the taxi fares is of great volume and our analysis involves multiple combinations of two,
	on average, datasets, as well as processing geospatial information, which ofthen involves computations 
	including big vectors, such as the information about boroughs' geometry, so we shall have to take care of 
	the available resources that may be needed for carrying out the analysis. This use case has been tested on 
	a standalone Spark cluster. Spark is very powerful in running applications using big data sets, even under 
	limited resources on a standard labtop, by following good practicies, such as caching appropriately the data,
	using broadcast joins when small datasets are involved in joins, choosing the number of workers so that the
	computations take full advantage of your resources and others.
	
	This particular type of analysis that involves the extraction of insights about an area of our interest 
	given a series of events over time that manifests it is ofthen called sessionization and it is often applied 
	to various logs' analysis. Despite its usefullness, it comes with great cost for its execution, as the data 
	is spread across rdd partitions and for its implementation extensive reshuffling of data is required. Spark 
	2.0 has brough some optimizations for reducing the cost of sessionization operations.


 
Code

	geospatial_and_temporal_analysis.py
   
   	All can be run interactively with pyspark shell or by submitting e.g. 
		exec(open("project/location/session_analysis/geospatial_and_temporal_analysis.py").read()) 
	for an all at once execution.     
	The code has been tested on a Spark standalone cluster. For the Spark setting, spark-3.1.2-bin-hadoop2.7 
	bundle has been used. The external python packages that are used in this implementation exist in the 
	requirements.txt file. Install with:      
	    	pip install -r project/location/session_analysis/requirements.txt    
    	This use case is inspired from the series of experiments presented in [8], though it deviates from it, in
	the programming language, the setting used and in the analysis followed.



References

	1. Brian Donovan and Daniel B. Work “New York City Taxi Trip Data (2010-2013)”. 1.0. University
       	   of Illinois at Urbana-Champaign. Dataset. http://dx.doi.org/10.13012/J8PN93H8, 2014.
	2. https://databricks.com/blog/2019/12/05/processing-geospatial-data-at-scale-with-databricks.html  
	3. https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc
	4. https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm
	5. https://sedona.apache.org/
	6. https://geopandas.org/
	7. https://spark.apache.org/docs/latest/api/python/index.html
	8. Advanced Analytics with Spark, Sandy Ryza, Uri Laserson, Sean Owen, & Josh Wills
	
