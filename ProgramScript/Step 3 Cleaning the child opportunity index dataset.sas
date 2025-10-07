/*
DISCLAIMER: READY-TO-RUN SCRIPT REQUIREMENTS

This script is syntactically correct, but it is NOT ready to run "out-of-the-box." It was
developed using external CSV files for the Child Opportunity Index (COI) and population data.

To use this script, you MUST:
1. Have access to the relevant COI data files.
2. Replace all placeholder paths (e.g., 'path/to/your/COI_data/')
   with the actual locations of YOUR datasets.

The code serves as a template for conducting this type of analysis.
*/

/*
TITLE: Step 3 - Cleaning the Child Opportunity Index (COI) Dataset

DESCRIPTION: This script imports and processes two datasets: the census tract-level COI data
and child population data. It filters both for Arkansas in 2015. It then calculates
population-weighted COI scores and aggregates child population counts by race at the county level.
Finally, it programmatically determines the majority race for each county.
*/


/* PART 0: LIBNAME STATEMENT */
/* Description: Define a library to store the COI datasets. */

libname coi 'path/to/your/project/COI_data';


/* PART 1: PROCESS CHILD OPPORTUNITY INDEX (COI) DATA */
/* Description: Import COI data and calculate population-weighted averages at the county level. */

/* Step 1.1: Import the raw COI data from the source CSV file. */
PROC IMPORT OUT=coi.ar_coi
     DATAFILE='path/to/your/COI_data/index.csv'
     DBMS=csv REPLACE;
     GETNAMES=YES;
     DATAROW=2;
     guessingrows=max;
RUN;

/* Step 1.2: Clean the COI data in a single step. */
/* This step filters for Arkansas (statefips=5) and the year 2015, converts numeric FIPS codes */
/* to character variables to preserve leading zeros, and drops unnecessary columns. */
DATA coi.ar_coi_clean;
   	SET coi.ar_coi;
   	WHERE (year=2015 and statefips=5);

	/* Convert numeric FIPS codes to character format. */
	geoid_char = PUT(geoid, z11.);
	countyfips_char = PUT(countyfips, z5.);
	statefips_char = PUT(statefips, z2.);

	/* Drop old numeric and unneeded national/state-level variables. */
	DROP geoid countyfips statefips year
		 c5_ED_nat c5_HE_nat c5_SE_nat c5_COI_nat c5_ED_stt c5_HE_stt c5_SE_stt c5_COI_stt
		 c5_ED_met c5_HE_met c5_SE_met c5_COI_met r_ED_met r_HE_met r_SE_met r_COI_met;
RUN;

/* Step 1.3: Calculate population-weighted COI scores at the county level. */
/* This aggregates the tract-level COI data up to the county level. Weighting by population */
/* ensures that tracts with more people have a proportionally greater influence on the county average. */
proc sql;
    create table coi.ar_coi_weighted as
    select
    	countyfips_char,
    	sum(pop) as county_pop,
		/* Calculate weighted averages for Z-scores and rankings at national and state levels */
    	sum(pop * z_ED_nat) / sum(pop) as weighted_avg_zednat,
    	sum(pop * z_HE_nat) / sum(pop) as weighted_avg_zhenat,
    	sum(pop * z_SE_nat) / sum(pop) as weighted_avg_zsenat,
    	sum(pop * z_COI_nat) / sum(pop) as weighted_avg_zcoinat,
    	sum(pop * r_ED_nat) / sum(pop) as weighted_avg_rednat,
    	sum(pop * r_HE_nat) / sum(pop) as weighted_avg_rhenat,
    	sum(pop * r_SE_nat) / sum(pop) as weighted_avg_rsenat,
    	sum(pop * r_COI_nat) / sum(pop) as weighted_avg_rcoinat,
    	sum(pop * r_ED_stt) / sum(pop) as weighted_avg_redstt,
    	sum(pop * r_HE_stt) / sum(pop) as weighted_avg_rhestt,
    	sum(pop * r_SE_stt) / sum(pop) as weighted_avg_rsestt,
    	sum(pop * r_COI_stt) / sum(pop) as weighted_avg_rcoistt
    from coi.ar_coi_clean
    group by countyfips_char;
quit;


/* PART 2: PROCESS CHILD POPULATION DATA */
/* Description: Import and aggregate child population data by race at the county level. */

/* Step 2.1: Import the raw child population data from the source CSV file. */
PROC IMPORT OUT=coi.ar_pop
     DATAFILE='path/to/your/COI_data/pop.csv'
     DBMS=csv REPLACE;
     GETNAMES=YES;
     DATAROW=2;
     guessingrows=max;
RUN;

/* Step 2.2: Clean the population data in a single step. */
/* This step filters for Arkansas and 2015, and converts FIPS codes to character variables. */
DATA coi.ar_pop_clean;
   	SET coi.ar_pop;
   	WHERE (year=2015 and statefips=5);

	geoid_char = PUT(geoid, z11.);
	countyfips_char = PUT(countyfips, z5.);
	statefips_char = PUT(statefips, z2.);

	drop geoid countyfips statefips year;
RUN;

/* Step 2.3: Aggregate child population by race/ethnicity at the county level. */
/* This query sums the tract-level counts to create county totals for each group and */
/* combines smaller race categories into a single 'other' group. */
proc sql;
	create table coi.ar_popsum as
    select
    	countyfips_char,
    	sum(hisp) as county_hisp,
    	sum(white) as county_white,
    	sum(black) as county_black,
    	/* Combine American Indian/Alaskan Native, Asian/Pacific Islander, and 2+ Races into one 'other' category */
    	sum(aian) + sum(api) + sum(other2) as county_other
    from coi.ar_pop_clean
    group by countyfips_char;
quit;


/* PART 3: DETERMINE MAJORITY RACE FOR EACH COUNTY */
/* Description: Programmatically calculate the majority race for each county. */
/* This is a robust replacement for the manual 'datalines' method in the original script. */
data coi.ar_popsum_final;
	set coi.ar_popsum;

	/* Create an array containing the population counts for each racial/ethnic group. */
	array races[4] county_hisp county_white county_black county_other;
	major_race = .; /* Initialize the new variable */

	/* Loop through the array to find which group has the highest population. */
	do i = 1 to 4;
		if races[i] = max(of races[*]) then do;
			major_race = i; /* Assign the corresponding code */
			leave; /* Exit the loop once the max is found */
		end;
	end;

	/*
	Code mapping for 'major_race':
	1 = Hispanic
	2 = Non-Hispanic White
	3 = Non-Hispanic Black
	4 = Non-Hispanic Other
	*/

	drop i; /* Drop the loop counter variable */
run;

/* END OF SCRIPT */