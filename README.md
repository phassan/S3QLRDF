# S3QLRDF (SPARQL to Spark SQL for RDF)
S3QLRDF is a distributed Hadoop-based SPARQL query processor for large-scale RDF data implemented on top of Spark. It uses the relational interface of Spark (Spark SQL) for query execution by compiling SPARQL to SQL and comes with a new partitioning schema for RDF called PTP (Property Table Partitioning) that is a modified and extended version of the well-known Property Table schema introduced by Wilkinson et al.

# Requirements
-	[Apache Hadoop](https://hadoop.apache.org/)
-	[Apache Spark](https://spark.apache.org/) 2+
-	[Flex](https://www.gnu.org/software/flex/) version 2.6.0
-	[Bison](https://www.gnu.org/software/bison/) version 3.0.4
-	[Clang](https://clang.llvm.org/index.html) 3.8

#  How to use it?
S3QLRDF has three modules: 
1. **Data Loader** is the module used to load RDF data into HDFS based on PTP storage schema. RDF data have to be in the [N-Triples format](https://www.w3.org/TR/n-triples/). It also generates a statistics file for each RDF dataset.

	  S3QLRDF DataLoader is a maven-scala project.
	
	  INSTALLATION:
	  ```
	  cd DataLoader
	  mvn package
	  ```
	  This module uses two steps to RDF load data in PTP schema. In the first step, the parameter **--schema** followed by **PT** to load       data in **Property Table** schema, and the second step uses the same parameter **--schema** followed by **PTP** to load data in           **Property Table Partitioning** schema. These two distinct steps can be executed separately with the following:
	
	  EXECUTION:
	  ```
	  First Step:
	  	./bin/spark-submit \
 	 	 	--class com.s3ql.dataLoader.Driver \
 	 	    	--master <master-url> \
 	 	    	--deploy-mode <deploy-mode> \
 		    	--conf <key>=<value> \
  		  	data-loader_2.11-0.1.jar \
  		  	--rdf-file hdfs-path-to-input-rdf-dataset \
  		  	--prefix-file path-to-the-prefix-file \
 		    	--output-dir /s3ql-PTP-storage-path-in-hdfs \
        	--dataset dataset-name (e.g. lubm) \
		    	--scale scale-factor-of-the-dataset (e.g. 1000. You can use 1 or anything if there is no scale factor) \
		    	--light-load yes \
		    	--schema PT 

	  Second Step: In this step you have to change only the storage schema from PT to PTP (from --schema PT to --schema PTP).

	  ```
	A statistics file `stat_<dataset>_<scale>_ptp.txt` will be created in the current directory after loading an RDF dataset.

2. **Query Translator** translates the SPARQL query into Spark SQL statements. It uses **Data Loader** generated statistics file to translate SPARQL query.
	  ```
    cd QueryTranslator/src
	  make compiler F=<path_to_statistics_file> D=<directory_contains_SPARQL_queries> O=<path_to_Spark_SQL_file>

	  For example:
	  make compiler F=/QueryTranslator/stat_file/stat_lubm_1000_ptp.txt D=/QueryTranslator/query O=/QueryTranslator/spark_sql.txt
	  ```

3. **Query Executor** runs the translated query with Apache Spark.
    S3QLRDF QueryExecutor is a maven-scala project.

	  INSTALLATION:
	  ```
	  cd QueryExecutor
	  mvn package
	  ```
	  EXECUTION:
	  ```
	  	./bin/spark-submit \
 	 	    	--class com.s3ql.queryExecutor.Driver \
 	 	   	--master <master-url> \
 		    	--deploy-mode <deploy-mode> \
 	 	    	--conf <key>=<value> \
  		   	query-executor_2.11-0.1.jar \
 		    	--output-dir /s3ql-PTP-storage-path-in-hdfs \
        	--dataset dataset-name \
		    	--scale scale-factor-of-the-dataset \
		    	--query-file spark_sql.txt \
		    	--cache-table no
	  ```
	  The arguments are as follows:
	  ```
	    --query-file <path_to_Spark_SQL_file>
	    --cache-table <Spark_cacheTable_fuctionality>
		    1. yes refers to cache table in memory before executing Spark SQL query.
		    2. no refers to not using Spark cacheTable functionality.
	  ```
## License
This project is under the [GPLv3](https://www.gnu.org/licenses/gpl-3.0.en.html) license.
