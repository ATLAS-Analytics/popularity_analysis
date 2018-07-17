
traces = LOAD '/user/lspiedel/rucio_expanded_2017/' USING PigStorage('\t') AS (
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

--split data into days
counts_days = FOREACH counts GENERATE FLOOR(time_diff/2592000L) as days, accesses, name;

first_month = FILTER counts_days BY days<30;
SPLIT first_month INTO 
    day0 IF (days==0),
    day1 IF (days==1), 
    day2 IF (days==2),
    day3 IF (days==3),
    day4 IF (days==4),
    day5 IF (days==5),
    day6 IF (days==6),
    day7 IF (days==7),  
    day8 IF (days==8),
    day9 IF (days==9), 
    day10 IF (days==10),
    day11 IF (days==11),
    day12 IF (days==12),
    day13 IF (days==13),
    day14 IF (days==14);
    


--define macro to find averages accesses per day per dataset
DEFINE average_acc(day_num, col_name) RETURNS x {
    --find total accesses per name
    group_name = GROUP $day_num BY name;
    $x = FOREACH group_name GENERATE group as name, SUM(${day_num}.accesses) as $col_name; 
    };
--work through days to find accesses then join to average_reduc alias
--outer join full ensures that all files accessed at least once in their first 7 days are included in analysis
average0 = average_acc(day0, 'acc_0');
average1 = average_acc(day1, 'acc_1');
average = JOIN average0 BY name FULL OUTER, average1 BY name;
average_reduc = FOREACH average GENERATE ((average0::name IS NULL) ? average1::name : average0::name) as name, 
    ((average0::acc_0 IS NULL) ? 0 : average0::acc_0) as acc_0, 
    ((average1::acc_1 IS NULL) ? 0 : average1::acc_1) as acc_1;

average2 = average_acc(day2, 'acc_2');
average = JOIN average_reduc BY name FULL OUTER, average2 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average2::name : average_reduc::name) as name, 
    acc_0, acc_1, 
    ((average2::acc_2 IS NULL) ? 0 : average2::acc_2) as acc_2;

average3 = average_acc(day3, 'acc_3');
average = JOIN average_reduc BY name FULL OUTER, average3 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average3::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2,
    ((average3::acc_3 IS NULL) ? 0 : average3::acc_3) as acc_3;

average4 = average_acc(day4, 'acc_4');
average = JOIN average_reduc BY name FULL OUTER, average4 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average4::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3,
    ((average4::acc_4 IS NULL) ? 0 : average4::acc_4) as acc_4;

average5 = average_acc(day5, 'acc_5');
average = JOIN average_reduc BY name FULL OUTER, average5 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average5::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5;

average6 = average_acc(day6, 'acc_6');
average = JOIN average_reduc BY name FULL OUTER, average6 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average6::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6;

-- for last day convert nulls into 0
average7 = average_acc(day7, 'acc_7');
average = JOIN average_reduc BY name FULL OUTER, average7 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average7::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7;

average8 = average_acc(day8, 'acc_8');
average = JOIN average_reduc BY name FULL OUTER, average8 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average8::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7, acc_8;

average9 = average_acc(day9, 'acc_9');
average = JOIN average_reduc BY name FULL OUTER, average9 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average9::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7, acc_8, acc_9;

average10 = average_acc(day10, 'acc_10');
average = JOIN average_reduc BY name FULL OUTER, average10 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average10::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7, acc_8, acc_9, acc_10;

average11 = average_acc(day11, 'acc_11');
average = JOIN average_reduc BY name FULL OUTER, average11 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average11::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7, acc_8, acc_9, acc_10, acc_11;

average12 = average_acc(day12, 'acc_12');
average = JOIN average_reduc BY name FULL OUTER, average12 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average12::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7, acc_8, acc_9, acc_10, acc_11, acc_12;

average13 = average_acc(day13, 'acc_13');
average = JOIN average_reduc BY name FULL OUTER, average13 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average13::name : average_reduc::name) as name, 
    acc_0, acc_1, acc_2, acc_3, acc_4, acc_5, acc_6, acc_7, acc_8, acc_9, acc_10, acc_11, acc_12, acc_13;

average14 = average_acc(day14, 'acc_14');
average = JOIN average_reduc BY name FULL OUTER, average14 BY name;
average_reduc = FOREACH average GENERATE ((average_reduc::name IS NULL) ? average14::name : average_reduc::name) as name,  
    ((acc_0 IS NULL) ? 0 : acc_0) as acc_0,
    ((acc_1 IS NULL) ? 0 : acc_1) as acc_1,
    ((acc_2 IS NULL) ? 0 : acc_2) as acc_2,
    ((acc_3 IS NULL) ? 0 : acc_3) as acc_3,
    ((acc_4 IS NULL) ? 0 : acc_4) as acc_4,
    ((acc_5 IS NULL) ? 0 : acc_5) as acc_5,
    ((acc_6 IS NULL) ? 0 : acc_6) as acc_6,
    ((acc_7 IS NULL) ? 0 : acc_7) as acc_7,
    ((acc_8 IS NULL) ? 0 : acc_8) as acc_8,
    ((acc_9 IS NULL) ? 0 : acc_9) as acc_9,
    ((acc_10 IS NULL) ? 0 : acc_10) as acc_10,
    ((acc_11 IS NULL) ? 0 : acc_11) as acc_11,
    ((acc_12 IS NULL) ? 0 : acc_12) as acc_12,
    ((acc_13 IS NULL) ? 0 : acc_13) as acc_13,
    ((average14::acc_14 IS NULL) ? 0 : average14::acc_14) as acc_14;

group_name = GROUP average_reduc ALL;
dist = FOREACH group_name {
    avg_0 = AVG(average_reduc.acc_0);
    avg_1 = AVG(average_reduc.acc_1);
    avg_2 = AVG(average_reduc.acc_2);
    avg_3 = AVG(average_reduc.acc_3);
    avg_4 = AVG(average_reduc.acc_4);
    avg_5 = AVG(average_reduc.acc_5);
    avg_6 = AVG(average_reduc.acc_6);
    avg_7 = AVG(average_reduc.acc_7);
    avg_8 = AVG(average_reduc.acc_8);
    avg_9 = AVG(average_reduc.acc_9);
    avg_10 = AVG(average_reduc.acc_10);
    avg_11 = AVG(average_reduc.acc_11);
    avg_12 = AVG(average_reduc.acc_12);
    avg_13 = AVG(average_reduc.acc_13);
    avg_14 = AVG(average_reduc.acc_14); 
    GENERATE avg_0, avg_1, avg_2, avg_3, avg_4, avg_5, avg_6, avg_7, avg_8, avg_9, avg_10, avg_11, avg_12, avg_13, avg_14; }

DUMP dist;
--ouput
STORE dist  INTO '/user/lspiedel/file_life/month12' USING PigStorage('\t');
