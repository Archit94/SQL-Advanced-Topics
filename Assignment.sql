#####################################
# Create schema
#####################################

create schema Assignment;

use Assignment;

# Imported tables through import table wizard

# Check the tables
show tables ;

################################################
# Check how many rows are Null in close price
################################################
select count(*) from bajaj where `close price` is NULL;
select count(*) from eicher where `close price` is NULL;
select count(*) from hero where `close price` is NULL;
select count(*) from infosys where `close price` is NULL;
select count(*) from tcs where `close price` is NULL;
select count(*) from tvs where `close price` is NULL;

#There are no nulls

########################
#Transforming to date
########################

update bajaj
set date = STR_TO_DATE(date, '%d-%M-%Y');

update eicher
set date = STR_TO_DATE(date, '%d-%M-%Y');

update infosys
set date = STR_TO_DATE(date, '%d-%M-%Y');

update hero
set date = STR_TO_DATE(date, '%d-%M-%Y');

update tcs
set date = STR_TO_DATE(date, '%d-%M-%Y');

update tvs
set date = STR_TO_DATE(date, '%d-%M-%Y');

##############################################################
# Making the 20-Day and 50-Day moving avg for all six stocks.
##############################################################

Create table bajaj1 as
	(SELECT date,
       `close price`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 19 PRECEDING) AS `20 Day MA`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 49 PRECEDING) AS `50 Day MA`
FROM   bajaj);

Create table eicher1 as
	(SELECT date,
       `close price`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 19 PRECEDING) AS `20 Day MA`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 49 PRECEDING) AS `50 Day MA`
FROM   eicher);

Create table infosys1 as
	(SELECT date,
       `close price`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 19 PRECEDING) AS `20 Day MA`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 49 PRECEDING) AS `50 Day MA`
FROM   infosys);

Create table hero1 as
	(SELECT date,
       `close price`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 19 PRECEDING) AS `20 Day MA`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 49 PRECEDING) AS `50 Day MA`
FROM   hero);

Create table tcs1 as
	(SELECT date,
       `close price`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 19 PRECEDING) AS `20 Day MA`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 49 PRECEDING) AS `50 Day MA`
FROM   tcs);

Create table tvs1 as
	(SELECT date,
       `close price`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 19 PRECEDING) AS `20 Day MA`,
       AVG(`close price`) OVER (ORDER BY date ASC ROWS 49 PRECEDING) AS `50 Day MA`
FROM   tvs);

######################################################################
######################################################################

####################################
# Creating the master table
####################################
create table master as (
select bajaj.date as Date,
	   bajaj.`close price` as Bajaj,
	   tcs.`close price` as TCS,
       tvs.`close price` as TVS,
	   infosys.`close price` as Infosys,
       eicher.`close price` as Eicher,
       hero.`close price` as Hero
	from bajaj join eicher using(date) 
    join infosys using(date) 
    join hero using(date)
    join tcs using(date) 
    join tvs using(date)
);

select * from master;

#####################################################################
#####################################################################

#################################################################
# Intermediate Function to generate the signal for Buy/Sell/Hold
#################################################################

delimiter $$
create function get_signal(MA_diff double,next_day double)
returns varchar(5) 	deterministic
begin
	declare Sign varchar(5);
    if MA_diff <= 0 and next_day > 0 then
		set Sign = 'BUY';
	elseif MA_diff >=0 and next_day < 0 then
		set Sign = 'SELL';
	else
		set Sign = 'HOLD';
	end if;
    return Sign;
	
end $$
delimiter ;

################################################################################
################################################################################

#######################
# Create signal tables
#######################

create table bajaj2 as (
# create a CTE for finding the MA differences(20 - 50 day MA diff)
# and also the next day MA diff
with bajaj_inter(Date, MA_diff, next_day) as (
select date,  `20 Day MA` - `50 Day MA` as MA_diff, lead (`20 Day MA` - `50 Day MA`,1,`20 Day MA` - `50 Day MA`) 
			over (order by date) as next_day
from bajaj1
)
# Use the CTE to finally fill a new column with signal values
select Date, `Close Price`, get_signal(MA_diff,next_day) as 'Signal'
from bajaj1 inner join bajaj_inter using(date)
);

create table eicher2 as (
# create a CTE for finding the MA differences(20 - 50 day MA diff)
# and also the next day MA diff
with eicher_inter(Date, MA_diff, next_day) as (
select date,  `20 Day MA` - `50 Day MA` as MA_diff, lead (`20 Day MA` - `50 Day MA`,1,`20 Day MA` - `50 Day MA`) 
			over (order by date) as next_day
from eicher1
)
# Use the CTE to finally fill a new column with signal values
select Date, `Close Price`, get_signal(MA_diff,next_day) as 'Signal'
from eicher1 inner join eicher_inter using(date)
);

create table hero2 as (
# create a CTE for finding the MA differences(20 - 50 day MA diff)
# and also the next day MA diff
with hero_inter(Date, MA_diff, next_day) as (
select date,  `20 Day MA` - `50 Day MA` as MA_diff, lead (`20 Day MA` - `50 Day MA`,1,`20 Day MA` - `50 Day MA`) 
			over (order by date) as next_day
from hero1
)
# Use the CTE to finally fill a new column with signal values
select Date, `Close Price`, get_signal(MA_diff,next_day) as 'Signal'
from hero1 inner join hero_inter using(date)
);

create table infosys2 as (
# create a CTE for finding the MA differences(20 - 50 day MA diff)
# and also the next day MA diff
with infosys_inter(Date, MA_diff, next_day) as (
select date,  `20 Day MA` - `50 Day MA` as MA_diff, lead (`20 Day MA` - `50 Day MA`,1,`20 Day MA` - `50 Day MA`) 
			over (order by date) as next_day
from infosys1
)
# Use the CTE to finally fill a new column with signal values
select Date, `Close Price`, get_signal(MA_diff,next_day) as 'Signal'
from infosys1 inner join infosys_inter using(date)
);

create table tcs2 as (
# create a CTE for finding the MA differences(20 - 50 day MA diff)
# and also the next day MA diff
with tcs_inter(Date, MA_diff, next_day) as (
select date,  `20 Day MA` - `50 Day MA` as MA_diff, lead (`20 Day MA` - `50 Day MA`,1,`20 Day MA` - `50 Day MA`) 
			over (order by date) as next_day
from tcs1
)
# Use the CTE to finally fill a new column with signal values
select Date, `Close Price`, get_signal(MA_diff,next_day) as 'Signal'
from tcs1 inner join tcs_inter using(date)
);

create table tvs2 as (
# create a CTE for finding the MA differences(20 - 50 day MA diff)
# and also the next day MA diff
with tvs_inter(Date, MA_diff, next_day) as (
select date,  `20 Day MA` - `50 Day MA` as MA_diff, lead (`20 Day MA` - `50 Day MA`,1,`20 Day MA` - `50 Day MA`) 
			over (order by date) as next_day
from tvs1
)
# Use the CTE to finally fill a new column with signal values
select Date, `Close Price`, get_signal(MA_diff,next_day) as 'Signal'
from tvs1 inner join tvs_inter using(date)
);


select * from tvs2;

###################################################################################
# User defined function for getting the signal on a date provided as <stock>_signal
# p_date -> date for which signal has to be retrieved
# select <stock>_signal('YYYY-MM-DD')
###################################################################################
delimiter $$
create function bajaj_signal(p_date date)
returns varchar(5) 	deterministic
begin
	declare sign varchar(5);
	select `Signal` INTO sign
    from bajaj2
    where date = p_date;
	return sign;
end $$

delimiter $$
create function eicher_signal(p_date date)
returns varchar(5) 	deterministic
begin
	declare sign varchar(5);
	select `Signal` INTO sign
    from eicher2
    where date = p_date;
	return sign;
end $$

delimiter $$
create function hero_signal(p_date date)
returns varchar(5) 	deterministic
begin
	declare sign varchar(5);
	select `Signal` INTO sign
    from hero2
    where date = p_date;
	return sign;
end $$

delimiter $$
create function infosys_signal(p_date date)
returns varchar(5) 	deterministic
begin
	declare sign varchar(5);
	select `Signal` INTO sign
    from infosys2
    where date = p_date;
	return sign;
end $$

delimiter $$
create function tcs_signal(p_date date)
returns varchar(5) 	deterministic
begin
	declare sign varchar(5);
	select `Signal` INTO sign
    from tcs2
    where date = p_date;
	return sign;
end $$

delimiter $$
create function tvs_signal(p_date date)
returns varchar(5) 	deterministic
begin
	declare sign varchar(5);
	select `Signal` INTO sign
    from tvs2
    where date = p_date;
	return sign;
end $$

delimiter ;

# Example call for infosys stock signal on a given date
select infosys_signal('2015-01-29') as 'Signal';


