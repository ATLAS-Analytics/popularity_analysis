
traces = LOAD '/user/lspiedel/tmp/rucio_expanded/' USING PigStorage('\t') AS (
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
	bytes:int,
	length:int,
	ops:int,
	file_ops:int,
	distinct_files:int,
	panda_jobs:int,
	created_at:long);

--reduce to needed fields
traces_reduc = FOREACH traces GENERATE name, timestamp;


--filter
filter_null_name = FILTER traces_reduc BY name IS NOT NULL AND name != '' AND name != 'null';
filter_null = FILTER filter_null_name BY timestamp IS NOT NULL;

--convert timestamp into unixtime in seconds
--if running off files in rucio01/tmp
time_conversion = FOREACH filter_null GENERATE name, GetDay(ToDate(timestamp, 'yyyy-MM-dd')) as timestamp;
--if running off files in lspiedel/
--time_conversion = FOREACH filter_null GENERATE name, GetDay(ToDate((long) timestamp*1000L)) as timestamp; 
filtered = LIMIT time_conversion 10;
DUMP filtered;
DESCRIBE time_conversion;

--generate counts for each name
group_time = GROUP time_conversion BY name;
counts = FOREACH group_time {
    sorted = ORDER time_conversion BY timestamp DESC;
    limited = LIMIT sorted 1;
    GENERATE FLATTEN(limited); }

--DESCRIBE counts;

group_timestamp = GROUP counts BY limited::timestamp;
time_dist = FOREACH group_timestamp {
    frequency = COUNT(counts.limited::name);
    GENERATE group, frequency; }

DUMP time_dist;
--ouput
--STORE time_dist INTO '/user/lspiedel/tmp/last_acc_2018-06_new' USING PigStorage('\t');
