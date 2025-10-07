/* PART 0: LIBNAME STATEMENTS */
/* Description: Define libraries for the database connection and local SAS datasets. */

/* Connect to your database via ODBC. Replace 'YOUR-DSN' with your data source name. */
libname _odbclib odbc noprompt="dsn=YOUR-DSN;Trusted_connection=yes;UseDeclareFetch=32767,schema=public";

/* Define libraries to access enrollment data and store claims and merged datasets. */
libname enroll 'path/to/your/project/enrollment_data';
libname claim 'path/to/your/project/claims_data';
libname merged 'path/to/your/project/merged_data';


/* PART 1: MEDICAL CLAIMS DATA PREPARATION */
/* Description: This section filters raw medical claims by age and identifies diagnoses of interest. */

/* Step 1.1: Apply age restriction (4-18 years) to each year's claims dataset. */
/* The birth year range is adjusted for each claims file to consistently select patients who were */
/* between 4 and 18 years old during that specific service year. */

/* For 2016 claims */
proc sql;
	create table claim.clm16_ageres (compress=yes) as
	select mc013_year, mc059, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2016
	where mc013_year between 1998 and 2012;
quit;

/* For 2017 claims */
proc sql;
	create table claim.clm17_ageres (compress=yes) as
	select mc013_year, mc059, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2017
	where mc013_year between 1999 and 2013;
quit;

/* For 2018 claims */
proc sql;
	create table claim.clm18_ageres (compress=yes) as
	select mc013_year, mc059, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2018
	where mc013_year between 2000 and 2014;
quit;

/* For 2019 claims */
proc sql;
	create table claim.clm19_ageres (compress=yes) as
	select mc013_year, mc059, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2019
	where mc013_year between 2001 and 2015;
quit;

/* For 2020 claims */
proc sql;
	create table claim.clm20_ageres (compress=yes) as
	select mc013_year, mc059, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2020
	where mc013_year between 2002 and 2016;
quit;

/* For 2021 claims */
proc sql;
	create table claim.clm21_ageres (compress=yes) as
	select mc013_year, mc059, mc041, mc042, mc043, mc044, mc045, mc046, mc047, mc048, mc049, mc050, mc051, mc052, mc053, MC915A, mc001, mc137
	from _odbclib.claim_svc_dt_2021
	where mc013_year between 2003 and 2017;
quit;

/* Step 1.2: Identify ADHD, ODD, and CD diagnosis cases in each age-restricted claims dataset. */
/* The logic uses an array to loop through all diagnosis code fields on each claim. */
/* It checks for both ICD-9 (MC915A=9) and ICD-10 (MC915A=0) codes. */
/* Note: Truncated diagnosis codes (e.g., 'F90', '314') act as wildcards. */
data claim.clm16_dx;
	set claim.clm16_ageres;
	svcyear = year(mc059); /* Create service year variable */
	adhd=0; odd=0; cd=0;   /* Initialize diagnosis flags */
	Array mcn mc041-mc053; /* Create an array of the diagnosis variables */
	j=1;
	Do j= 1 to 13;
		If mcn(j) in: ('F90') and MC915A=0 then adhd=1;
		If mcn(j) in: ('314') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F910','F911','F912','F918','F919','F63') and MC915A=0 then cd=1; /* F63=Impulse disorders */
		If mcn(j) in: ('312') and MC915A=9 then cd=1; /* Includes 312.3=Impulse disorders */
		If mcn(j) in: ('F913') and MC915A=0 then cd=0; /* Exclude ODD from the CD definition */
	end;
	drop j;
run;

data claim.clm17_dx;
	set claim.clm17_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	j=1;
	Do j= 1 to 13;
		If mcn(j) in: ('F90') and MC915A=0 then adhd=1;
		If mcn(j) in: ('314') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F910','F911','F912','F918','F919','F63') and MC915A=0 then cd=1;
		If mcn(j) in: ('312') and MC915A=9 then cd=1;
		If mcn(j) in: ('F913') and MC915A=0 then cd=0;
	end;
	drop j;
run;

data claim.clm18_dx;
	set claim.clm18_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	j=1;
	Do j= 1 to 13;
		If mcn(j) in: ('F90') and MC915A=0 then adhd=1;
		If mcn(j) in: ('314') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F910','F911','F912','F918','F919','F63') and MC915A=0 then cd=1;
		If mcn(j) in: ('312') and MC915A=9 then cd=1;
		If mcn(j) in: ('F913') and MC915A=0 then cd=0;
	end;
	drop j;
run;

data claim.clm19_dx;
	set claim.clm19_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	j=1;
	Do j= 1 to 13;
		If mcn(j) in: ('F90') and MC915A=0 then adhd=1;
		If mcn(j) in: ('314') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F910','F911','F912','F918','F919','F63') and MC915A=0 then cd=1;
		If mcn(j) in: ('312') and MC915A=9 then cd=1;
		If mcn(j) in: ('F913') and MC915A=0 then cd=0;
	end;
	drop j;
run;

data claim.clm20_dx;
	set claim.clm20_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	j=1;
	Do j= 1 to 13;
		If mcn(j) in: ('F90') and MC915A=0 then adhd=1;
		If mcn(j) in: ('314') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F910','F911','F912','F918','F919','F63') and MC915A=0 then cd=1;
		If mcn(j) in: ('312') and MC915A=9 then cd=1;
		If mcn(j) in: ('F913') and MC915A=0 then cd=0;
	end;
	drop j;
run;

data claim.clm21_dx;
	set claim.clm21_ageres;
	svcyear = year(mc059);
	adhd=0; odd=0; cd=0;
	Array mcn mc041-mc053;
	j=1;
	Do j= 1 to 13;
		If mcn(j) in: ('F90') and MC915A=0 then adhd=1;
		If mcn(j) in: ('314') and MC915A=9 then adhd=1;
		If mcn(j) in: ('F913') and MC915A=0 then odd=1;
		If mcn(j) in: ('31381') and MC915A=9 then odd=1;
		If mcn(j) in: ('F910','F911','F912','F918','F919','F63') and MC915A=0 then cd=1;
		If mcn(j) in: ('312') and MC915A=9 then cd=1;
		If mcn(j) in: ('F913') and MC915A=0 then cd=0;
	end;
	drop j;
run;

/* Step 1.3: Combine the yearly claims files and filter for the study period. */
/* First, append all processed yearly claims files. */
data claim.clm16to19_dx;
	set claim.clm16_dx
		claim.clm17_dx
		claim.clm18_dx
		claim.clm19_dx
		claim.clm20_dx
		claim.clm21_dx;
	if (adhd=1 or odd=1 or cd=1) then output; /* Keep only claims with a diagnosis of interest */
	proc sort;
		by svcyear;
run;

/* Second, subset the combined file to keep only claims from the 2016-2019 study period. */
data claim.clm16to19_dx;
	set claim.clm16to19_dx;
	if (2016<=svcyear<=2019) then output;
run;


/* PART 2: MERGING ENROLLMENT AND CLAIMS DATA */
/* Step 2.1: Merge the enrollment and claims files using an inner join. */
/* This step creates a dataset of "cases": individuals who are in the enrollment file AND have at least one claim with a relevant diagnosis. */
proc sql;
	create table merged.mrgdinr as
	select *
	from enroll.enrl_racefixed a
	inner join
	claim.clm16to19_dx b
	on a.me001 = b.mc001 and a.me107 = b.mc137;
quit;

/* The following commented-out block checks for missing IDs. */
/*
proc sql;
select nmiss(me998) as me998_miss
    from merged.mrgdinr;
quit;
*/

/* Count the number of unique individuals in the merged "cases" file. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from merged.mrgdinr);
	title 'Number of individuals in the merged File';
quit;

/* Step 2.2: Collapse the claims data to the person-year level. */
/* First, standardize the year variable name. */
data merged.mrgdinr2;
	set merged.mrgdinr;
	drop year;
	rename svcyear=year;
	proc sort;
		by me998 me013 year;
run;

/* Second, collapse the data to create one record per person per year. */
/* The max function ensures the diagnosis flag is 1 if they had any relevant claim during that year. */
proc means data = merged.mrgdinr2 noprint;
	by me998 me013 year;
	var adhd odd cd;
	output out = merged.mrgd_collapse (drop = _type_ _freq_)
	max = adhd odd cd;
run;

/* Step 2.3: Join the person-year case data back to the full enrollment cohort. */
/* A left join is used to keep ALL individuals from the enrollment file (the denominator). */
/* Individuals without a diagnosis will have missing values for the adhd, odd, and cd flags. */
proc sql;
	create table merged.mrgdfinal as
	select *
	from enroll.enrl_racefixed a
	left join
	merged.mrgd_collapse b
	on a.me998 = b.me998 and a.me013 = b.me013 and a.year=b.year;
quit;

/* Step 2.4: Recode missing diagnosis flags to zero. */
/* This correctly identifies individuals without a diagnosis as non-cases (0) instead of missing. */
proc stdize data=merged.mrgdfinal
	out=merged.mrgdfinal2
	reponly missing=0;
        var adhd odd cd;
run;

/* Step 2.5: Create a summary variable for having any of the target diagnoses. */
data merged.mrgdfinal2;
	set merged.mrgdfinal2;
	if (adhd=1 or odd=1 or cd=1) then anymhd=1;
	else anymhd=0;
run;

/* Final count of unique individuals in the complete analytic dataset. */
/* This count should match the number of individuals in the final enrollment file. */
proc sql;
	select count(*) as n from (select distinct me998, me013 from merged.mrgdfinal2);
	title 'Number of individuals in the merged File';
quit;

/* END OF SCRIPT */