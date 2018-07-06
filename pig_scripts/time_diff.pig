REGISTER udf.py USING streaming_python AS funcs;

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
	created_at:float);

--reduce to needed fields
traces_reduc = FOREACH traces GENERATE eventtype, name, ops, created_at, timestamp;

--filter
filter_null = FILTER traces_reduc BY name != 'NULL';


--generate counts for each name
group_name = GROUP filter_null BY (name, created_at, timestamp);
counts = FOREACH group_name {
	job_number = SUM(filter_null.ops);
	age = funcs.convert_date(group.timestamp);   
    time_diff = SUBTRACT(age, group.created_at);
    GENERATE group.name as name, time_diff as time_diff, job_number as accesses; }

--find distribution
group_count = GROUP counts BY time_diff;
dist = FOREACH group_count {
    accesses = SUM(counts.accesses);
	GENERATE group as time_diff, accesses as count; }

DUMP dist
--output result
--STORE dist INTO '/user/lspiedel/tmp/time_diff/test_l' USING PigStorage('\t');
