--TARGET: TO GET CREATE THE DATABASE ADN LOAD THE FILES


--creates a database pagerank
create database if not exists pagerank;



--creates a table for link_graph which is the result of mapreduce output.
CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.link_graph(pi int, pj int) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'; 


--loads data in table pagerank
LOAD DATA LOCAL INPATH '/home/administrator/Desktop/link_graph' overwrite into table pagerank.link_graph;

--select * from pagerank.link_graph limit 19;
--****************************************************************************************
--TARGET: TO GET ALL THE INCOMMING LINKS TO THE PI FROM LINK GRAPH And the count of out
--going number of liknks to incoming link
-- PI|IN_COMMINGLINKS|L(INCOMINGLINK)

--reversing the link graph
CREATE VIEW IF NOT EXISTS pagerank.r_graph as select pj as pi, pi as pj from pagerank.link_graph;

--distinct pi
CREATE VIEW IF NOT EXISTS pagerank.distpi as select DISTINCT pi from pagerank.link_graph;

--group by pj
CREATE VIEW IF NOT EXISTS pagerank.grouppj as select pj as pj , COUNT(*) as cnt from pagerank.link_graph GROUP BY pj;

--All the incomming links for each pi are found.(null values exists)
CREATE VIEW IF NOT EXISTS pagerank.incoming_links as Select l.pi , r.pj from pagerank.distpi l left outer join pagerank.r_graph r on (l.pi = r.pi); 

--find all the pi and their outlink count
--group by pi to get thett incoming link to pis
create external table if not exists pagerank.grpi(pi int, cnt int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE pagerank.grpi 
select pi as pi, COUNT(pi) as cnt from pagerank.link_graph group by pi;

--three coulmn tables is found. pi|incoming|countofin_outof pi

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.link_table(pi int, incomming_link int, outgoing_picnt int) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'; 
 

insert overwrite table pagerank.link_table
select inc.pi, inc.pj, gpi.cnt from pagerank.incoming_links inc left outer join pagerank.grpi gpi on (inc.pj = gpi.pi);


--******************************************************************************************
--TARGET: TO GET A FOUR COULMNED TABLE PI|IN_LINKPI|OUT_IN_CNT|INTIAL PR

--find the N in citation network
CREATE VIEW IF NOT EXISTS pagerank.rankcal as select COUNT(DISTINCT pi) as tot from pagerank.link_graph ;

--two colums pi|intial_pagerank ( 1/totalN)
CREATE VIEW IF NOT EXISTS pagerank.pagerank_table as select DISTINCT l.pi as pindex, (1/rc.tot) as pr from pagerank.link_graph l join pagerank.rankcal rc;


--create a column table with all teh pagerank with it target achived.
CREATE VIEW IF NOT EXISTS pagerank.rank_table as select lt.pi as pi, lt.incomming_link as in_link, lt.outgoing_picnt as out_cnt,  (1/rc.tot) as pr from pagerank.link_table lt join pagerank.rankcal rc;

--******************************************************************************************
--TARGET: ROUNDS OF OPERNATION TO COMPUTE THE PAGERANK CALCULATION.
--ROUND-1: PR(PI)/L(PJ) 
--ROUND-2:MULTIPLY WITH 0.85(DAMPING FACTOR
--ROUND-3: REPLACING NULL VALUES WITH (1-D)/N AS THEY DONT HAVE ANY INCOMMIN.
--ROUND-4:ADDING THE (1-D)/N TO PAGERANKS(PR)
--round1:
create view if not exists pagerank.round1 as select rk.pi as pi, rk.in_link as in_link, rk.out_cnt as out_cnt, (rk.pr/rk.out_cnt) as pr from pagerank.rank_table rk;

--SUM AND GOROUP BY
create view if not exists pagerank.temround1 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.round1 fr group by fr.pi;

--round2:
--multiplying with Damping factor
CREATE VIEW IF NOT EXISTS pagerank.round2 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.temround1 fr;


set N = 564705;

--adding the (1-d)/n,INTIAL PAGERANK ACHIVED.
CREATE VIEW IF NOT EXISTS pagerank.round3 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.round2 rt;

-- a veiw that contain the (1-d)/n value in it
create view if not exists pagerank.const as select ((1-0.85)/f.tot) as constd from pagerank.rankcal f;

--temp join to get a extra column of 1-d value to later replace null values
create view if not exists pagerank.tempround as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.round3 rk join pagerank.const c;

--replace null values with the orginal rank.
create view if not exists pagerank.finalrank as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.tempround tr;

--******************************************************************************************
--*******************ITERATION-1**************************
---table set up for next itration -1
Create view if not exists pagerank.round5 as select lt.pi as pi, lt.incomming_link as in_link, lt.outgoing_picnt as outin_cnt, rt.pr from pagerank.link_table lt left outer join pagerank.finalrank rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration11 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round5 rk;


--calucaitng sum(pr)
create view if not exists pagerank.iteration12 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration11 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration13 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration12 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration14 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration13 rt;

--addding constant
create view if not exists pagerank.iteration15 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration14 rk join pagerank.const c;

--removing null
create view if not exists pagerank.finaliteration1 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration15 tr

--Select * from pagerank.finaliteration1;
--******************************************************************************************
--*******************ITERATION-2**************************

--table set up for next iteration-2
Create view if not exists pagerank.round6 as select lt.pi as pi, lt.incomming_link as in_link, lt.outgoing_picnt as outin_cnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration1 rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration21 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round6 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration22 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration21 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration23 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration22 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration24 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration23 rt;

--adding constant for null
create view if not exists pagerank.iteration25 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration24 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration2 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration25 tr;




--select * from pagerank.finaliteration2

--******************************************************************************************

--*******************ITERATION-3**************************

--table set up for next iteration-3
Create view if not exists pagerank.round7 as select lt.pi as pi, lt.incomming_link as in_link, lt.outgoing_picnt as outin_cnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration2 rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration31 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round7 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration32 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration31 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration33 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration32 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration34 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration33 rt;

--adding constant for null
create view if not exists pagerank.iteration35 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration34 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration3 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration35 tr;



--select * from pagerank.finaliteration3


--******************************************************************************************

--*******************ITERATION-4**************************


--table set up for next iteration-4

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round8(pi int, in_link int, outin_cnt int, pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert OVERWRITE TABLE pagerank.round8
select lt.pi, lt.incomming_link, lt.outgoing_picnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration3 rt on (lt.incomming_link = rt.pi);


--calculating pr/lpi
create view if not exists pagerank.iteration41 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round8 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration42 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration41 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration43 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration42 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration44 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration43 rt;

--adding constant for null
create view if not exists pagerank.iteration45 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration44 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration4 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration45 tr;





--******************************************************************************************

--*******************ITERATION-5**************************


--table set up for next iteration-5


CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round9(pi int, in_link int, outin_cnt int, pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert OVERWRITE TABLE pagerank.round9
select lt.pi, lt.incomming_link, lt.outgoing_picnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration4 rt on (lt.incomming_link = rt.pi);



--calculating pr/lpi
create view if not exists pagerank.iteration51 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round9 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration52 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration51 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration53 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration52 fr;

--adding (1-d)/n
create view if not exists pagerank.iteration54 as select rt.pi AS pi ,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration53 rt;

--adding constant for null
create view if not exists pagerank.iteration55 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration54 rk join pagerank.const c;

--elimnating null
CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.finaliteration5(pi int, pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';


INSERT OVERWRITE TABLE pagerank.finaliteration5 
Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration55 tr;




--******************************************************************************************

--*******************ITERATION-6**************************

--table set up for next iteration-6

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round10(pi int, in_link int, outin_cnt int, pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert OVERWRITE TABLE pagerank.round10
select lt.pi, lt.incomming_link, lt.outgoing_picnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration5 rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration61 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round10 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration62 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration61 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration63 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration62 fr;

--add(1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration64 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration63 rt;

--adding constant for null
create view if not exists pagerank.iteration65 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration64 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration6 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration65 tr;






--******************************************************************************************

--*******************ITERATION-7**************************

--table set up for next iteration-7
CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round11(pi int, in_link int, outin_cnt int,pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert overwrite table pagerank.round11
select lt.pi, lt.incomming_link, lt.outgoing_picnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration6 rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration71 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round11 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration72 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration71 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration73 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration72 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration74 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration73 rt;

--adding constant for null
create view if not exists pagerank.iteration75 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration74 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration7 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration75 tr;





--******************************************************************************************

--*******************ITERATION-8**************************

--table set up for next iteration-8

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round12(pi int, in_link int, outin_cnt int,pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert overwrite table pagerank.round11 
select lt.pi, lt.incomming_link, lt.outgoing_picnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration7 rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration81 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round12 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration82 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration81 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration83 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration82 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration84 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration83 rt;

--adding constant for null
create view if not exists pagerank.iteration85 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration84 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration8 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration85 tr;





--******************************************************************************************

--*******************ITERATION-9**************************

--table set up for next iteration-9

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round13(pi int, in_link int, outin_cnt int,pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';

insert overwrite table pagerank.round13
select lt.pi as pi, lt.incomming_link as in_link, lt.outgoing_picnt as outin_cnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration8 rt on (lt.incomming_link = rt.pi);

--calculating pr/lpi
create view if not exists pagerank.iteration91 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round13 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration92 as SELECT fr.pi as pi, SUM(fr.pr) as pr from pagerank.iteration91 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration93 as select fr.pi as pi, 0.85*fr.pr as pr from pagerank.iteration92 fr;

--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration94 as select rt.pi as pi,((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration93 rt;

--adding constant for null
create view if not exists pagerank.iteration95 as select rk.pi as pi, rk.pr as pr, c.constd as cnst from pagerank.iteration94 rk join pagerank.const c;

--eliminating null
create view if not exists pagerank.finaliteration9 as Select tr.pi as pi, case when isnull(tr.pr) then tr.cnst else tr.pr end as pr from pagerank.iteration95 tr;





--******************************************************************************************

--*******************ITERATION-10**************************
--table set up for next iteration-10

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.round14(pi int, in_link int, outin_cnt int,pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert overwrite table pagerank.round14
select lt.pi, lt.incomming_link, lt.outgoing_picnt, rt.pr from pagerank.link_table lt left outer join pagerank.finaliteration9 rt on (lt.incomming_link = rt.pi);


--calculating pr/lpi
create view if not exists pagerank.iteration101 as select rk.pi as pi, rk.in_link as in_link, rk.outin_cnt as out_cnt, (rk.pr/rk.outin_cnt) as pr from pagerank.round14 rk;

--calucaitng sum(pr)
create view if not exists pagerank.iteration102 as SELECT fr.pi as pi, COUNT(pi) AS cit_cnt, SUM(fr.pr) as pr from pagerank.iteration101 fr group by fr.pi;

--multiplying dampingfactor
CREATE VIEW IF NOT EXISTS pagerank.iteration103 as select fr.pi as pi,fr.cit_cnt as cit_out , 0.85*fr.pr as pr from pagerank.iteration102 fr;


--adding (1-d)/n
CREATE VIEW IF NOT EXISTS pagerank.iteration104 as select rt.pi as pi,rt.cit_out as cit_cnt, ((1-0.85)/${hiveconf:N})+rt.pr as pr from pagerank.iteration103 rt;

--adding constant for null
create view if not exists pagerank.iteration105 as select rk.pi as pi, rk.cit_cnt as cit_cnt, rk.pr as pr, c.constd as cnst from pagerank.iteration104 rk join pagerank.const c;

--eliminating null

CREATE EXTERNAL TABLE IF NOT EXISTS pagerank.final_pagerank10(pi int, cit_cnt int,pr double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

insert overwrite table pagerank.final_pagerank10 
Select tr.pi,tr.cit_cnt, case when isnull(tr.pr) then tr.cnst else tr.pr end from pagerank.iteration105 tr;



insert overwrite local directory '/home/administrator/Desktop/pagerank_final'
row format delimited
fields terminated by '\t' 
Select * from pagerank.final_pagerank10 l order by l.pr DESC limit 10 ;

















