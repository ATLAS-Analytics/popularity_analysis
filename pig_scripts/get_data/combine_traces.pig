REGISTER /usr/lib/pig/lib/avro.jar;
REGISTER /usr/lib/avro/avro-mapred.jar;

--script to load in rucio_popularity files and at created_at for each dataset

--load in rucio_popularity file for date
traces = LOAD '/user/rucio01/tmp/rucio_popularity/$DATE/' USING PigStorage('\t') AS (
	timestamp:chararray,
	user:chararray,
	scope:chararray,
	name:chararray,
	project:chararray,
	datatype:chararray,
	run_number:int,
	stream_name:chararray,
	prod_step:chararray,
	version:chararray,
	eventtype:chararray,
	rse:chararray,
	bytes:long,
	length:int,
	ops:int,
	file_ops:int,
	distinct_files:int,
	panda_jobs:int);

--load in did for date
dids = LOAD '/user/rucio01/dumps/${DATE}/dids' USING AvroStorage();
read_dataset_dids = FILTER dids BY DID_TYPE == 'D';

--reduce did to fields needed
reduce_fields_dids = FOREACH dids GENERATE SCOPE as scope, NAME as name, CREATED_AT as created_at;
--add the values from the did to the traces
join_traces_dids = JOIN traces BY (scope, name), reduce_fields_dids BY (scope, name);

--generate final columns with name
add_meta = FOREACH join_traces_dids GENERATE traces::timestamp, traces::user as user, traces::scope as scope, traces::name as name, traces::project as project, traces::datatype as datatyp, traces::run_number as run_number, traces::stream_name as stream_name, traces::prod_step as prod_step, traces::version as version, traces::eventtype as eventtype, traces::rse as rse, traces::bytes as bytes, traces::length as length, traces::ops as ops, traces::file_ops as file_ops, traces::distinct_files as distinct_files, traces::panda_jobs as panda_jobs, reduce_fields_dids::created_at as created_at;

--order and store
output_ordered = ORDER add_meta BY timestamp ASC, scope ASC, name ASC, user ASC, eventtype ASC;
STORE output_ordered INTO '/user/lspiedel/rucio_expanded_2017/${DATE}/' USING PigStorage('\t');
