#!/bin/sh

port=$1
dbname=$2
branch=$3
nruns=20

function run_tests
{
	context=$1
	sql1=$2
	sql2=$3

	for r in `seq 1 $nruns`; do

		psql -p $port -c 'drop table if exists t1' $dbname > /dev/null 2>&1
		psql -p $port -c 'drop table if exists t2' $dbname > /dev/null 2>&1

		psql -p $port -c 'create table t1 (a int, b int)' $dbname > /dev/null
		psql -p $port -c 'create table t2 (a int, b int)' $dbname > /dev/null

		psql -p $port $dbname > /dev/null <<EOF
SELECT setseed($r::numeric/$nruns::numeric);
$sql1
EOF
		psql -p $port $dbname > /dev/null <<EOF
SELECT setseed($r::numeric/$nruns::numeric);
$sql2
EOF

		psql -p $port -c "vacuum analyze t1" $dbname > /dev/null
		psql -p $port -c "vacuum analyze t2" $dbname > /dev/null

		psql -p $port -c "checkpoint" $dbname > /dev/null

		psql -t -A -p $port $dbname > count.$branch.log <<EOF
SET enable_nestloop=off;
SELECT COUNT(*) FROM t1 JOIN t2 ON (t1.a = t2.a);
EOF

		actual=`cat count.$branch.log | tail -n 1`

		for stats_target in 10 100 1000 10000; do

			psql -p $port $dbname -c "alter table t1 alter a set statistics $stats_target" > /dev/null
			psql -p $port $dbname -c "alter table t2 alter a set statistics $stats_target" > /dev/null

			psql -p $port -c "analyze t1" $dbname > /dev/null
			psql -p $port -c "analyze t2" $dbname > /dev/null

			mcvlen1=`psql -t -A -p $port $dbname -c "select array_length(most_common_vals,1) from pg_stats where tablename = 't1' and attname = 'a'"`
			mcvfreq1=`psql -t -A -p $port $dbname -c "select sum(f) from (select unnest(most_common_freqs) AS f from pg_stats where tablename = 't1' and attname = 'a') foo"`

			mcvlen2=`psql -t -A -p $port $dbname -c "select array_length(most_common_vals,1) from pg_stats where tablename = 't2' and attname = 'a'"`
			mcvfreq2=`psql -t -A -p $port $dbname -c "select sum(f) from (select unnest(most_common_freqs) AS f from pg_stats where tablename = 't2' and attname = 'a') foo"`

			ndist1_est=`psql -t -A -p $port $dbname -c "select (CASE WHEN n_distinct > 0 THEN n_distinct ELSE (SELECT (-n_distinct * reltuples)::bigint FROM pg_class WHERE relname = 't1') END) from pg_stats where tablename = 't1' and attname = 'a'"`
			ndist2_est=`psql -t -A -p $port $dbname -c "select (CASE WHEN n_distinct > 0 THEN n_distinct ELSE (SELECT (-n_distinct * reltuples)::bigint FROM pg_class WHERE relname = 't2') END) from pg_stats where tablename = 't2' and attname = 'a'"`

			ndist1_cnt=`psql -t -A -p $port $dbname -c "select count(distinct a) from t1"`
			ndist2_cnt=`psql -t -A -p $port $dbname -c "select count(distinct a) from t2"`

			psql -p $port $dbname > explain.$branch.log <<EOF
EXPLAIN SELECT * FROM t1 JOIN t2 ON (t1.a = t2.a);
EOF

			estimate=`grep '\(Join\|Loop\)' explain.$branch.log | sed 's/.*rows=\([0-9]\+\).*/\1/g'`

			echo `date +%s`,$branch,$context,$stats_target,$r,$mcvlen1,$mcvlen2,$mcvfreq1,$mcvfreq2,$ndist1_est,$ndist2_est,$ndist1_cnt,$ndist2_cnt,$estimate,$actual

		done

	done
}

echo "timestamp,branch,type,nrows1,nrows2,ndistinct1,ndistinct2,stats_target,run,mcvlen1,mcvlen2,mcvfreq1,mcvfreq2,ndist1_est,ndist2_est,ndist1_cnt,ndist2_cnt,estimate,actual"

for nrows1 in 1000 10000 100000; do

	for nrows2 in 1000 10000 100000; do

		for ndistinct1 in 100 1000 10000; do

			for ndistinct2 in 100 1000 10000; do

				# UNIFORM

				sql1="insert into t1 select random() * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select random() * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "uniform,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# SKEWED

				sql1="insert into t1 select pow(random(),2) * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select pow(random(),2) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "pow2,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# SKEWED (inverse)

				sql1="insert into t1 select pow(random(),2) * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select (1 - pow(random(),2)) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "pow2-inverse,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# UNIFORM-SKEWED

				sql1="insert into t1 select random() * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select pow(random(),2) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "uniform-pow2,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# UNIFORM-SKEWED (inverse)

				sql1="insert into t1 select random() * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select (1 - pow(random(),2)) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "uniform-pow2-inverse,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# SKEWED-STRONG

				sql1="insert into t1 select pow(random(),4) * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select pow(random(),4) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "pow4,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# SKEWED-STRONG (inverse)

				sql1="insert into t1 select pow(random(),4) * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select (1 - pow(random(),4)) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "pow4-inverse,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# UNIFORM-SKEWED-STRONG

				sql1="insert into t1 select random() * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select pow(random(),4) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "uniform-pow4,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# UNIFORM-SKEWED-STRONG (inverse)

				sql1="insert into t1 select random() * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select (1 - pow(random(),4)) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "uniform-pow4-inverse,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# SKEWED-SKEWED-STRONG

				sql1="insert into t1 select pow(random(),2) * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select pow(random(),4) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "pow2-pow4,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

				# SKEWED-SKEWED-STRONG (inverse)

				sql1="insert into t1 select pow(random(),2) * $ndistinct1, i FROM generate_series(1,$nrows1) s(i)"
				sql2="insert into t2 select (1 - pow(random(),4)) * $ndistinct2, i FROM generate_series(1,$nrows2) s(i)"

				run_tests "pow2-pow4-inverse,$nrows1,$nrows2,$ndistinct1,$ndistinct2" "$sql1" "$sql2"

			done

		done

	done

done

