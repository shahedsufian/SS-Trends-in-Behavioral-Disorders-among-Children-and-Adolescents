/*
DISCLAIMER: READY-TO-RUN SCRIPT REQUIREMENTS

This script is syntactically correct, but it is NOT ready to run "out-of-the-box." It was
developed using a final analytic dataset created in prior steps.

To use this script, you MUST:
1. Have successfully run the preceding data preparation scripts.
2. Replace all placeholder paths (e.g., 'path/to/your/output/') with the actual
   locations for your output files.

The code serves as a template for conducting this type of analysis.
*/

/*
TITLE: Step 4 - Generating Output and Final Analysis

DESCRIPTION: This script uses the final merged analytic file to perform several analyses.
It calculates county-level prevalence for 2019 for both the total and Medicaid-only
populations, generates descriptive cross-tabulations, exports trend data to Excel,
and runs logistic regression models to test for trends and interactions.
*/


/* PART 0: LIBNAME STATEMENTS */
/* Description: Define libraries for the database connection and local SAS datasets. */

/* Connect to your database via ODBC. Replace 'YOUR-DSN' with your data source name. */
libname _odbclib odbc noprompt="dsn=YOUR-DSN;Trusted_connection=yes;UseDeclareFetch=32767,schema=public";

/* Define libraries to access previously created datasets. */
libname enroll 'path/to/your/project/enrollment_data';
libname claim 'path/to/your/project/claims_data';
libname merged 'path/to/your/project/merged_data';


/* PART 1: DATA PREPARATION FOR 2019 ANALYSIS */
/* Description: Create analysis variables and a subset of the data for the year 2019. */

/* Step 1.1: Create categorical variables for analysis. */
data merged.mrgdfinal2_cats;
	set merged.mrgdfinal2;
	/* Create age category: 0 = (4-11 years), 1 = (12-17 years) */
	if 4<=age<=11 then agecat=0;
	else if 12<=age<=17 then agecat=1;
	/* Create sex variable: 0 = Female, 1 = Male */
	if male=0 then sex=0;
	else if male=1 then sex=1;
	/* Create insurance coverage variable: 0 = Public, 1 = Private */
	if (medicare=1 or medicaid=1) then inscov=0;
	else if (medicare=0 and medicaid=0) then inscov=1;
run;

/* Step 1.2: Create a clean dataset for the 2019 cross-sectional analysis. */
data merged.mrgd19;
	set merged.mrgdfinal2_cats;
	/* Filter for the year 2019 and relevant age range. */
	if (year=2019 and 4<=age<=17);
	/* Remove records with invalid state or county codes. */
	if (me016 ne "05" or me173a_5digit="05000") then delete;
	/* Create a single categorical race variable. */
	if race_hispanic_new = 1 then race = 1;      /* 1=Hispanic */
	else if race_white_new = 1 then race = 2; /* 2=Non-Hispanic White */
	else if race_black_new = 1 then race = 3; /* 3=Non-Hispanic Black */
	else if race_other_new = 1 then race = 4; /* 4=Non-Hispanic Other */
	else race = 0;                            /* 0=Unknown */
run;


/* PART 2: COUNTY-LEVEL PREVALENCE (TOTAL POPULATION, 2019) */
/* Description: Calculate and rank disorder prevalence for each county in Arkansas for 2019. */

/* Step 2.1: Calculate the count of children with each disorder by county. */
proc means data=merged.mrgd19 noprint;
	class me173a_5digit;
	var adhd odd cd anymhd;
	output out=merged.county_n
		sum(adhd)=adhd_count
		sum(odd)=odd_count
		sum(cd)=cd_count
		sum(anymhd)=anymhd_count;
run;

/* Step 2.2: Calculate the prevalence (mean) of each disorder by county. */
proc means data=merged.mrgd19 nway missing;
	class me173a_5digit;
	var adhd odd cd anymhd;
	output out=merged.county_pvln
		mean(adhd)=adhd_pvln
		mean(odd)=odd_pvln
		mean(cd)=cd_pvln
		mean(anymhd)=anymhd_pvln;
run;

/* Step 2.3: Merge the count and prevalence datasets. */
data merged.county;
	merge merged.county_pvln merged.county_n;
	by me173a_5digit;
	if me173a_5digit="" then delete;
	drop _TYPE_;
	rename _FREQ_=FREQ;
run;

/* Step 2.4: Rank counties into quintiles based on prevalence for each disorder. */
/* Ranking is restricted to counties with more than 10 cases for statistical stability. */
proc rank data=merged.county out=merged.anymhd_ranked groups=5 ties=mean;
   var anymhd_pvln;
   ranks anymhd_qntl;
run;
proc rank data=merged.county (where=(adhd_count > 10)) out=merged.adhd_ranked groups=5 ties=mean;
   var adhd_pvln;
   ranks adhd_qntl;
run;
proc rank data=merged.county (where=(odd_count > 10)) out=merged.odd_ranked groups=5 ties=mean;
   var odd_pvln;
   ranks odd_qntl;
run;
proc rank data=merged.county (where=(cd_count > 10)) out=merged.cd_ranked groups=5 ties=mean;
   var cd_pvln;
   ranks cd_qntl;
run;

/* Step 2.5: Merge the ranked datasets and prepare for joining with other data. */
data merged.county_qntl;
	merge merged.anymhd_ranked merged.adhd_ranked merged.odd_ranked merged.cd_ranked;
	by me173a_5digit;
run;
data merged.county_qntl;
	set merged.county_qntl;
  	rename me173a_5digit=countyfips_char;
run;


/* PART 3: COUNTY-LEVEL PREVALENCE (MEDICAID POPULATION, 2019) */
/* Description: Repeat the prevalence calculation for the Medicaid-only sub-population. */

/* Step 3.1: Create a subset of the 2019 data for children with Medicaid. */
data merged.medicaid19;
	set merged.mrgd19;
	if medicaid ne 1 then delete;
run;

/* Step 3.2: Calculate disorder counts by county for the Medicaid population. */
proc means data=merged.medicaid19 noprint;
	class me173a_5digit;
	var adhd odd cd anymhd;
	output out=merged.mdcaid_county_n
		sum(adhd)=adhd_count sum(odd)=odd_count sum(cd)=cd_count sum(anymhd)=anymhd_count;
run;

/* Step 3.3: Calculate disorder prevalence by county for the Medicaid population. */
proc means data=merged.medicaid19 nway missing;
	class me173a_5digit;
	var adhd odd cd anymhd;
	output out=merged.mdcaid_county_pvln
		mean(adhd)=adhd_pvln mean(odd)=odd_pvln mean(cd)=cd_pvln mean(anymhd)=anymhd_pvln;
run;

/* Step 3.4: Merge the Medicaid count and prevalence datasets. */
data merged.mdcaid_county;
	merge merged.mdcaid_county_pvln merged.mdcaid_county_n;
	by me173a_5digit;
	if me173a_5digit="" then delete;
	drop _TYPE_;
	rename _FREQ_=FREQ;
run;

/* Step 3.5: Rank counties based on prevalence within the Medicaid population. */
proc rank data=merged.mdcaid_county out=merged.mdcaid_anymhd_rnk groups=5 ties=mean;
   var anymhd_pvln;
   ranks anymhd_qntl;
run;
proc rank data=merged.mdcaid_county (where=(adhd_count > 10)) out=merged.mdcaid_adhd_rnk groups=5 ties=mean;
   var adhd_pvln;
   ranks adhd_qntl;
run;
proc rank data=merged.mdcaid_county (where=(odd_count > 10)) out=merged.mdcaid_odd_rnk groups=5 ties=mean;
   var odd_pvln;
   ranks odd_qntl;
run;
proc rank data=merged.mdcaid_county (where=(cd_count > 10)) out=merged.mdcaid_cd_rnk groups=5 ties=mean;
   var cd_pvln;
   ranks cd_qntl;
run;

/* Step 3.6: Merge the ranked datasets for the Medicaid population. */
data merged.mdcaid_cnty_qntl;
	merge merged.mdcaid_anymhd_rnk merged.mdcaid_adhd_rnk merged.mdcaid_odd_rnk merged.mdcaid_cd_rnk;
	by me173a_5digit;
run;

/* Create a smaller final file for mapping Medicaid prevalence. */
data merged.mdcaid_map;
	set merged.mdcaid_cnty_qntl;
	keep me173a_5digit freq anymhd_count anymhd_pvln anymhd_qntl;
	rename me173a_5digit=countyfips_char;
run;


/* PART 4: DESCRIPTIVE STATISTICS AND TREND ANALYSIS */
/* Description: Generate frequency tables for prevalence by county, demographics, and year. */

/* Step 4.1: Generate prevalence tables by county for each disorder (2019). */
proc surveyfreq data=merged.mrgd19;
	table adhd*me173a_5digit/cl col chisq;
	title 'Prevalence of Diagnosed ADHD by County, 2019';
run;
proc surveyfreq data=merged.mrgd19;
	table odd*me173a_5digit/cl col chisq;
	title 'Prevalence of Diagnosed ODD by County, 2019';
run;
proc surveyfreq data=merged.mrgd19;
	table cd*me173a_5digit/cl col chisq;
	title 'Prevalence of Diagnosed CD by County, 2019';
run;
proc surveyfreq data=merged.mrgd19;
	table anymhd*me173a_5digit/cl col chisq;
	title 'Prevalence of Any Behavioral Disorder by County, 2019';
run;

/* Step 4.2: Generate prevalence tables by sociodemographic variables (2019). */
proc surveyfreq data=merged.mrgd19;
	table adhd*(agecat sex inscov)/cl col chisq;
	title 'Prevalence of Diagnosed ADHD by Demographics, 2019';
run;
proc surveyfreq data=merged.mrgd19;
	table odd*(agecat sex inscov)/cl col chisq;
	title 'Prevalence of Diagnosed ODD by Demographics, 2019';
run;
proc surveyfreq data=merged.mrgd19;
	table cd*(agecat sex inscov)/cl col chisq;
	title 'Prevalence of Diagnosed CD by Demographics, 2019';
run;
proc surveyfreq data=merged.mrgd19;
	table anymhd*(agecat sex inscov)/cl col chisq;
	title 'Prevalence of Any Behavioral Disorder by Demographics, 2019';
run;

/* Step 4.3: Export prevalence trends from 2013-2019 to an Excel file. */
ODS TAGSETS.EXCELXP
	file='path/to/your/output/behavioral_disorder_trends.xls'
	STYLE=minimal
	OPTIONS ( Orientation = 'landscape' FitToPage = 'yes' );

/* Generate prevalence tables by year for each disorder. */
proc surveyfreq data=merged.mrgdfinal2_cats;
	table adhd*year/cl col chisq;
	where 4<=age<=17 and year<2020;
	title 'Prevalence of Diagnosed ADHD, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table odd*year/cl col chisq;
	where 4<=age<=17 and year<2020;
	title 'Prevalence of Diagnosed ODD, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table cd*year/cl col chisq;
	where 4<=age<=17 and year<2020;
	title 'Prevalence of Diagnosed CD, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table anymhd*year/cl col chisq;
	where 4<=age<=17 and year<2020;
	title 'Prevalence of Any Behavioral Disorder, 2013-2019';
run;

ods tagsets.excelxp close;

/* Step 4.4: Generate stratified prevalence trends over time. */
/* ADHD trends stratified by age, sex, and insurance. */
proc surveyfreq data=merged.mrgdfinal2_cats;
	table agecat*adhd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'ADHD Prevalence Trends by Age Group, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table sex*adhd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'ADHD Prevalence Trends by Sex, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table inscov*adhd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'ADHD Prevalence Trends by Insurance Type, 2013-2019';
run;

/* ODD trends stratified by age, sex, and insurance. */
proc surveyfreq data=merged.mrgdfinal2_cats;
	table agecat*odd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'ODD Prevalence Trends by Age Group, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table sex*odd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'ODD Prevalence Trends by Sex, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table inscov*odd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'ODD Prevalence Trends by Insurance Type, 2013-2019';
run;

/* CD trends stratified by age, sex, and insurance. */
proc surveyfreq data=merged.mrgdfinal2_cats;
	table agecat*cd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'CD Prevalence Trends by Age Group, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table sex*cd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'CD Prevalence Trends by Sex, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table inscov*cd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'CD Prevalence Trends by Insurance Type, 2013-2019';
run;

/* Any behavioral disorder trends stratified by age, sex, and insurance. */
proc surveyfreq data=merged.mrgdfinal2_cats;
	table agecat*anymhd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'Any BD Prevalence Trends by Age Group, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table sex*anymhd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'Any BD Prevalence Trends by Sex, 2013-2019';
run;
proc surveyfreq data=merged.mrgdfinal2_cats;
	table inscov*anymhd*year/cl col chisq; where 4<=age<=17 and year<2020;
	title 'Any BD Prevalence Trends by Insurance Type, 2013-2019';
run;


/* PART 5: LOGISTIC REGRESSION MODELING */
/* Description: Assess the statistical significance of prevalence trends over time. */

/* Step 5.1: Run main effects models to assess overall trends from 2013-2019. */
/* These models test if the 'year' variable is a significant predictor of diagnosis, */
/* controlling for demographic factors. */
proc surveylogistic data=merged.mrgdfinal2_cats;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model adhd (descending) = year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Logistic Model for ADHD Prevalence Trend, 2013-2019';
run;
proc surveylogistic data=merged.mrgdfinal2_cats;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model odd (descending) = year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Logistic Model for ODD Prevalence Trend, 2013-2019';
run;
proc surveylogistic data=merged.mrgdfinal2_cats;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model cd (descending) = year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Logistic Model for CD Prevalence Trend, 2013-2019';
run;
proc surveylogistic data=merged.mrgdfinal2_cats;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model anymhd (descending) = year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Logistic Model for Any Behavioral Disorder Prevalence Trend, 2013-2019';
run;

/* Step 5.2: Create interaction terms to test for differing trends across strata. */
data merged.mrgdfinal3;
	set merged.mrgdfinal2_cats;
	ageintx=year*agecat;
	sexintx=year*sex;
	inscovintx=year*inscov;
run;

/* Step 5.3: Run interaction models for each disorder. */
/* These models test if the trend over time is significantly different for various */
/* demographic subgroups (e.g., does the trend for ADHD differ by sex?). */

/* ADHD Interaction Models */
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model adhd (descending) = ageintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'ADHD Trend Interaction with Age Group';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model adhd (descending) = sexintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'ADHD Trend Interaction with Sex';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model adhd (descending) = inscovintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'ADHD Trend Interaction with Insurance';
run;

/* ODD Interaction Models */
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model odd (descending) = ageintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'ODD Trend Interaction with Age Group';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model odd (descending) = sexintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'ODD Trend Interaction with Sex';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model odd (descending) = inscovintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'ODD Trend Interaction with Insurance';
run;

/* CD Interaction Models */
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model cd (descending) = ageintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'CD Trend Interaction with Age Group';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model cd (descending) = sexintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'CD Trend Interaction with Sex';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model cd (descending) = inscovintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'CD Trend Interaction with Insurance';
run;

/* Any Behavioral Disorder Interaction Models */
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model anymhd (descending) = ageintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Any BD Trend Interaction with Age Group';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model anymhd (descending) = sexintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Any BD Trend Interaction with Sex';
run;
proc surveylogistic data=merged.mrgdfinal3;
	class agecat (ref="0") sex (ref="0") inscov (ref="0");
	model anymhd (descending) = inscovintx year agecat sex inscov;
	where 4<=age<=17 and year<2020;
	title 'Any BD Trend Interaction with Insurance';
run;

/* END OF SCRIPT */