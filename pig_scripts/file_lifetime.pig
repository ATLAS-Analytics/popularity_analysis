
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
traces_reduc = FOREACH traces GENERATE name, ops, created_at, timestamp;

--filter
filter_null_name = FILTER traces_reduc BY name IS NOT NULL AND name != '' AND name != 'Null';
filter_null = FILTER filter_null_name BY created_at IS NOT NULL;

--convert both times into unixtime in seconds
time_conversion = FOREACH filter_null GENERATE name, ops, created_at/1000L as created_at, ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd')) as timestamp;
--time_conversion = FOREACH filter_null GENERATE name, ops, created_at/1000L as created_at, timestamp;


--generate counts for each name
group_time = GROUP time_conversion BY (name, created_at, timestamp);
counts = FOREACH group_time {
    job_number = SUM(time_conversion.ops);
    time_diff = group.timestamp - group.created_at;
    GENERATE group.name as name, time_diff as time_diff, job_number as accesses; }

--split data into days and aggregate for each file over each day
counts_days = FOREACH counts GENERATE FLOOR(time_diff/2629746L) as days, accesses, name;

first_month = FILTER counts_days BY days<30;
SPLIT first_month INTO 
    day0 IF (days==0),
    day1 IF (days==1), 
    day2 IF (days==2),
    day3 IF (days==3),
    day4 IF (days==4),
    day5 IF (days==5),
    day6 IF (days==6),
    day7 IF (days==7); 

--define macro to find average number of accesses on that day
DEFINE average_acc(day_num, col_name) RETURNS x {
    --find total accesses per name
    group_name = GROUP $day_num BY name;
    agg_name = FOREACH group_name GENERATE group as name, SUM(${day_num}.accesses) as accesses;
    --find average number of accesses of each file
    grouped = GROUP agg_name ALL;
    $x = FOREACH grouped GENERATE AVG(agg_name.accesses) as $col_name, 1 as join_var; 
    };

average0 = average_acc(day0, 'day0_avg');
average1 = average_acc(day1, 'day1_avg');
average2 = average_acc(day2, 'day2_avg');
average3 = average_acc(day3, 'day3_avg');
average4 = average_acc(day4, 'day4_avg');
average5 = average_acc(day5, 'day5_avg');
average6 = average_acc(day6, 'day6_avg');
average7 = average_acc(day7, 'day7_avg');

joined = JOIN average0 BY join_var, average1 BY join_var, average2 BY join_var, average3 BY join_var, average4 BY join_var, average5 BY join_var, average6 BY join_var, average7 BY join_var;
joined_norepeat = FOREACH joined GENERATE day0_avg, day1_avg, day2_avg, day3_avg, day4_avg, day5_avg, day6_avg, day7_avg;

DUMP joined_norepeat;
--ouput
--STORE counts_aggregated  INTO '/user/lspiedel/tmp/dist_by_age_year' USING PigStorage('\t');
