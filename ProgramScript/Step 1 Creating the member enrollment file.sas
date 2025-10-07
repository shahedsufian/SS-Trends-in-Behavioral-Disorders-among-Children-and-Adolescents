/* PART 0: LIBNAME STATEMENTS */
/* Description: Define libraries for the database connection and local SAS datasets. */

/* Connect to your database via ODBC. Replace 'YOUR-DSN' with your data source name. */
libname _odbclib odbc noprompt="dsn=YOUR-DSN;Trusted_connection=yes;UseDeclareFetch=32767,schema=public";

/* Define a library to store the member enrollment datasets. */
libname enroll 'path/to/your/project/enrollment_data';

/* PART 1: DATA INGESTION AND INITIAL FILTERING */
/* Description: Ingest raw member data and apply initial inclusion/exclusion criteria. */

/* Step 1.1: Count the total number of unique members in the raw file as a baseline. */
proc sql;
	select count(*) as n from (select distinct me013, me998 from _odbclib.member);
	title 'Number of entries in Arkansas APCD (me998+gender) with no restrictions';
quit;

/* Step 1.2: Restrict the sample to the pediatric population of interest. */
/* Selects members with a birth year between 1998 and 2015 to create a cohort of */
/* children aged 4-18 during the 2016-2019 study period. */
proc sql;
	create table enroll.enrl_ageres (compress=yes) as
	select*
	from _odbclib.member
	where me014_year between 1998 and 2015; /* ME014_YEAR Member Year of Birth */
quit;

/* Step 1.3: Count unique children after the age restriction has been applied. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enroll.enrl_ageres);
	title 'Number of Children in Eligibility File after Age Restriction (4-18 years) has been applied';
quit;

/* Step 1.4: Exclude members without medical coverage or with non-relevant insurance plans. */
/* ME018=1 indicates medical coverage. */
/* ME003 codes "AW" (Workers' Comp) and "DNT" (Dental) are excluded. */
data enroll.enrl_ageres;
	set enroll.enrl_ageres;
	if (me018 ne 1 or me003="AW" or me003="DNT") then delete;
run;

/* Step 1.5: Count unique children after age and coverage restrictions. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enroll.enrl_ageres);
	title 'Number of Children in Eligibility File after Age Restriction (4-18 years) has been applied';
quit;

/* PART 2: CREATION OF CATEGORICAL AND FLAG VARIABLES */
/* Description: Create new variables for demographics and yearly enrollment status. */

/* Step 2.1: Create age, gender, insurance, and initial race variables. */
proc sql;
	create table enroll.enrl_catarg as
	select *,
		/* Create age variables for each year of the study period. */
		(2019 - me014_year) as age_2019,
		(2018 - me014_year) as age_2018,
		(2017 - me014_year) as age_2017,
		(2016 - me014_year) as age_2016,
		/* Create dummy variables for gender. */
		case when me013 in ('M') then 1 else 0 end as male,
		case when me013 in ('F') then 1 else 0 end as female,
		/* Create dummy variables for insurance type. */
		case when me001_cat in ('PRIV') then 1 else 0 end as private,
		case when me001_cat in ('MCR') then 1 else 0 end as medicare,
		case when me001_cat in ('MCD') then 1 else 0 end as medicaid,
		/* Create initial dummy variables for race/ethnicity. */
		case when me025 in ('13' '14' '15' '16' '17' '18' '19' '20' '21' '22' '34') then 1 else 0 end as hispanic_race, /* ME025 Member Ethnicity 1 */
		case when me021 in ('2106-3') then 1 else 0 end as white_race, /* ME021 Member Race 1 */
		case when me021 in ('2054-5') then 1 else 0 end as black_race,
		case when me021 in ('1002-5' '2028-9' '2076-8' '2131-1') then 1 else 0 end as other_race,
		case when me021 in ('9999-9') then 1 else 0 end as unknown_race
	from enroll.enrl_ageres;
quit;

/* Step 2.2: Recreate the table, adding flags for enrollment in each year from 2016 to 2019. */
/* A member is flagged if they were enrolled for at least one day in a given year. */
proc sql;
	create table enroll.enrl_catarg as
	select *,
		case when  me162a<= '31Dec16'd and me163a>= '01Jan16'd then 1 else 0 end as en16,
		case when  me162a<= '31Dec17'd and me163a>= '01Jan17'd then 1 else 0 end as en17,
		case when  me162a<= '31Dec18'd and me163a>= '01Jan18'd then 1 else 0 end as en18,
		case when  me162a<= '31Dec19'd and me163a>= '01Jan19'd then 1 else 0 end as en19
	from enroll.enrl_catarg;
quit;

/* To identify data type of variables. */
proc contents data=enroll.enrl_catarg;
run;

/* Count the number of unique individuals in the eligibility file at this stage. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enroll.enrl_catarg);
	title 'Number of individuals in Eligibility File';
quit;

/* Check for missing values in county variables. */
proc freq data = enroll.enrl_catarg;
	tables me153a me173a;
run;

/* PART 3: DATA RESTRUCTURING TO PERSON-YEAR FORMAT */
/* Description: Reshape the data from one-row-per-enrollment-spell to one-row-per-person-per-year. */

/* Step 3.1: Create a single unique member ID by concatenating ID fields. */
data enroll.enrl_catarg;
	set enroll.enrl_catarg;
	me998_013 = CATS(me998, me013);
run;

/* Step 3.2: Create year-wise datasets to have unique observations, keeping the earliest enrollment date. */
/* For 2016 */
data enroll.en16_catarg;
	set enroll.enrl_catarg;
	if en16=1 then output; /* Filter for members enrolled in 2016 */
	rename age_2016=age;
	proc sort;
		by me998_013 me162a;
run;

data enroll.en16_catarg;
	set enroll.en16_catarg;
	by me998_013;
	if first.me998_013; /* Keep only one record per person for this year */
	year=2016; /* Add a year indicator */
run;

/* For 2017 */
data enroll.en17_catarg;
	set enroll.enrl_catarg;
	if en17=1 then output;
	rename age_2017=age;
	proc sort;
		by me998_013 me162a;
run;

data enroll.en17_catarg;
	set enroll.en17_catarg;
	by me998_013;
	if first.me998_013;
	year=2017;
run;

/* For 2018 */
data enroll.en18_catarg;
	set enroll.enrl_catarg;
	if en18=1 then output;
	rename age_2018=age;
	proc sort;
		by me998_013 me162a;
run;

data enroll.en18_catarg;
	set enroll.en18_catarg;
	by me998_013;
	if first.me998_013;
	year=2018;
run;

/* For 2019 */
data enroll.en19_catarg;
	set enroll.enrl_catarg;
	if en19=1 then output;
	rename age_2019=age;
	proc sort;
		by me998_013 me162a;
run;

data enroll.en19_catarg;
	set enroll.en19_catarg;
	by me998_013;
	if first.me998_013;
	year=2019;
run;

/* Step 3.3: Stack the four yearly datasets into a single person-year file. */
data enroll.en16to19_catarg;
	set enroll.en16_catarg enroll.en17_catarg enroll.en18_catarg enroll.en19_catarg;
run;

/* Count unique children in the final person-year dataset. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enroll.en16to19_catarg);
	title 'Number of individuals in Eligibility File';
quit;

/* PART 4: FINAL VARIABLE CREATION AND CLEANING */
/* Description: Create final, mutually exclusive race variables and county FIPS codes. */

/* Step 4.1: Create final, mutually exclusive race categories from the initial flags. */
data enroll.enrl_racefixed;
	set enroll.en16to19_catarg;
	/* Non-Hispanic White */
	race_white_new = 0;
	if white_race = 1 and hispanic_race = 0 then race_white_new = 1;
	/* Non-Hispanic Black */
	race_black_new = 0;
	if black_race = 1 and hispanic_race = 0 then race_black_new = 1;
	/* Hispanic (any race) */
	race_hispanic_new = hispanic_race;
	/* Non-Hispanic Other */
	race_other_new = 0;
	if other_race = 1 and hispanic_race = 0 then race_other_new = 1;
	/* Unknown/Missing */
	race_missing_new = 0;
	if race_white_new = 0 and race_black_new = 0 and race_hispanic_new = 0 and race_other_new = 0 then race_missing_new = 1;
	/* Drop the old, non-mutually exclusive race variables. */
	drop white_race black_race hispanic_race other_race unknown_race;
run;

/* Step 4.2: Remove members with a missing county code. */
data enroll.enrl_racefixed;
	set enroll.enrl_racefixed;
	if me173a='' then delete; /* ME173A = Member County */
run;

/* Count unique individuals in the final analytic file. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from enroll.enrl_racefixed);
	title 'Number of individuals in Eligibility File';
quit;

/* Step 4.3: Create a 5-digit county FIPS code. */
/* First, create a character variable for the Arkansas state FIPS code. */
data enroll.enrl_racefixed;
	set enroll.enrl_racefixed;
	arstatefips='05';
run;

/* Second, concatenate the state FIPS code with the county code. */
data enroll.enrl_racefixed;
	set enroll.enrl_racefixed;
	me173a_5digit = CATS(arstatefips, me173a);
run;

