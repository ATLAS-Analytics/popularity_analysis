
traces = LOAD '/user/lspiedel/tmp/test_l/' USING PigStorage('\t') AS (
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
traces_reduc = FOREACH traces GENERATE name, ops, created_at, timestamp;

--filter
filter_null_name = FILTER traces_reduc BY name IS NOT NULL AND name != '' AND name != 'Null';
filter_null = FILTER filter_null_name BY created_at IS NOT NULL AND ops IS NOT NULL;

--convert both times into unixtime in seconds
time_conversion = FOREACH filter_null GENERATE name, ops, created_at/1000L as created_at, ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd')) as timestamp;
DESCRIBE time_conversion;

--generate counts for each name
group_name = GROUP time_conversion BY (name, created_at, timestamp);
counts = FOREACH group_name {
    job_number = SUM(time_conversion.ops);
    time_diff = group.timestamp - group.created_at;
    GENERATE time_diff as time_diff, job_number as accesses; }

--sort data into bins
counts_days = FOREACH counts GENERATE FLOOR(time_diff/86400L) as days, time_diff, accesses;

--group by day and find total number of accesses
day_groups = GROUP counts_days BY days;
counts_aggregated = FOREACH day_groups {
    freq = SUM(counts_days.accesses);
    GENERATE group as bin, freq as freq; }

--ouput
STORE counts_aggregated  INTO '/user/lspiedel/tmp/dist_by_age1' USING PigStorage('\t');
