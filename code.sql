/*
This file contains the response to the Atlassian Code Assignment
Author: Parth Chandarana
*/

/*
How to best read the file?
The file has got 3 sections. Each section outlines what it does. To support reader, I have tried my best to write inline comments.
*/

-------------------------------------------------- START OF SECTION 1 -------------------------------------------------------
/*
Note: Before starting this section, please go through the readme.md file. It provides list of required tools to run the project.

Section 1: Prepare the Infrastructure.
Section Description: This section creates the source table, target table and load the data into source table.
source table is populated with data from extract file which was shared as part of assignment.
target table schema is created. More details about source and target table schemas are mentioned in the readme file.
*/

-- Step 1: This step will created the table required for loading the initial data from the source file(s).
create table IF NOT EXISTS jira_issues (time_string varchar, unix_time bigint, instance varchar, 
product varchar, username varchar, event varchar, attributes text);


-- Step 2: This step will create the target table with required schema. The schema decsision is based on what sort of questions we would like to answer. 
create table if not exists users (issue_viewed_at varchar(30), customer_id varchar(100), 
end_user_id varchar(100), user_action varchar(100), issue_key varchar(50), issue_id varchar(50));


-- Step 3: Load the tsv file from the mounted docker volume into newly created source table. 
-- Please Note: The below query acts as a Data Ingestion mechanism.
-- ***PLEASE NOTE*** the path to tsv file should be of yours. If jira_clean.tsv is placed at some different location and you are not using dockeized PostgreSQL 
-- then use the path where jira_clean.tsv is available.
COPY jira_issues FROM '/shared_folder/jira_clean.tsv' WITH delimiter E'\t';


-- Step 4: Initial transformation.
-- The below query acts as an initial transformation. It removes single quote(') from JSON like text column. 
-- This is required because single quote has special meaning in PostgreSQL.
-- Therefore, if removed we can use parrten matching to extract whichever field is required from attributes column.
update jira_issues set attributes = replace (attributes, '''', '');


-- View initial data from newly populated table.
select * from jira_issues;

-------------------------------------------------- END OF SECTION 1 ---------------------------------------------------------

-------------------------------------------------- START OF SECTION 2 -------------------------------------------------------
/*
Section 2: ETL using Stored Procedure and data reconciliation after load.
Section Description: This section defines PostgreSQL Stored Procedure.
It first extracts the data from jira_issues table, then transforms (rename columns, ger required values from attributes column) 
and load into users (target) table.
*/

/*
Procedure to extract data from jira_issues table.
Some of the columns are directly mapped to target table schema.
Other are extracted from attributes column. Based on what to answer, other columns can be derived.

Input: 
	name: start_date type: varchar(10)
	name: end_date   type: varchar(10)
Output:
	NA

Records between these two dates will be extracted from jira_issues and inserted into users table. This feature will help this ETL to be more robust.
*/

create or replace procedure populate_users(
   start_date varchar(10), end_date varchar(10)
)
language plpgsql    
as $$
begin
    -- subtracting the amount from the sender's account 
    insert into users
	SELECT time_string as issue_viewed_at, instance as customer_id, username as end_user_id, event as user_action, 
	substring(trim(substring(attributes  from '%issueKey:#"%#"[,|}]%' for '#')), 1, 32) as issue_key, 
	substring(trim(substring(attributes  from '%issueId:#"%#"[,|}]%' for '#')), 1, 32) as issue_id
	from jira_issues where substring(time_string, 1, 10)  between start_date and end_date;
    commit;
end;$$


-- This is a procedure call. Data will not be loaded into target table until the above stroed procedure is called with appropriate date.
call populate_users('2014-12-01', '2014-12-02');

-- Senity check whether data loaded or not.
select * from users;


-- After calling the above stored proc, we will have to verify the data load and the following recon query can help.
select count(*) from users; 
-- Two days worth of data records. 26260

select count(*) from jira_issues where substring(time_string, 1, 10) between  '2014-12-01' and '2014-12-02'; 
-- Same number of records 26260. The ETL is working fine and loading correct number of records.

-- Call procedure again to load rest of the days data or delete from target table and load all days data at once. Here I have loaded remaining days data.
call populate_users('2014-12-03', '2014-12-31');

-------------------------------------------------- END OF SECTION 2 ---------------------------------------------------------

-------------------------------------------------- START OF SECTION 3 -------------------------------------------------------
/*
Section 3: Answer Given Queries and comments for future enhancement.
Section Description: This section attempts to answer the given queries in the assignment.
It provides the queries and explanation.
*/

/*
Q1 Which users are viewing the most issues?  (hint - the view event is called kickass.viewIssue)
The view event attributes has got a field called issueId. I believe this field will tell us user is viewing which issue. 
Once decided, we just need to do couple of aggregations and join to get the desired output.
*/
------ QUERY
with distinct_issues as(
select issue_id, count(1) as issues_count from users where user_action = 'kickass.viewIssue' group by issue_id order by count(1) desc 
),
-- select * from distinct_issues /* This query is just to see the intermediate output from 1st aggregate*/
user_issues as (
select customer_id, end_user_id, issue_id, count(1) as user_issues from users where user_action = 'kickass.viewIssue' group by customer_id, end_user_id, issue_id
)
--select * from user_issues /* This query is just to see the intermediate output from 1st aggregate*/
select customer_id, end_user_id, distinct_issues.issue_id, user_issues.user_issues, distinct_issues.issues_count from user_issues join distinct_issues 
on user_issues.issue_id = distinct_issues.issue_id 
--where user_issues.issue_id = '2e3881af8bd791893d704096acd1e751' -- This filter condition is to view/test/verify output for any particular issue_id.
order by issues_count desc, user_issues desc;


/*
Q2 Are users viewing lots of distinct issues, or the same issue? (hint - the column attributes has an identifier called issueKey)
*/

-- To Answer this question, first we need to check how many records out of total where issue_key is present
select count(*) from users where issue_key is not null; 

/*
1852 records out of 592458. not a lot of records. ~0.31% records
*/

--Now check distinct issue_key
select count(distinct (issue_key)) from users where issue_key is not null; 
/* 1259 records. 
 * This tells us that users might be viewing different issues.
 * However, lets verify that using below Aggregate query 
 */ 

-- Get aggregation by end_user_id and issue_key
select 
--customer_id, --This may or may not required based on further analysis of which end_user from customer is viewing the issue
end_user_id, issue_key, count(1) as issue_key_count from users where issue_key is not null group by 
--customer_id, 
end_user_id, issue_key order by count(1) desc;


-- To go even further do one more level of aggregate.
select issue_key_count, count(1) from (
select end_user_id, issue_key, count(1) as issue_key_count from users where issue_key is not null group by 
end_user_id, issue_key order by count(1) desc) a group by a.issue_key_count order by issue_key_count desc;

/*
The outcome suggest that issue_count 1 is observed by highest 1219 times. Total view count where issueKey is present is 1852. 
This means that lot of people are viewing the distinct issues.
*/


/*
Q3. Are the most active users new users, or old users? (hint - it’s safe to assume the dataset begins on Dec 1st 2014)
*/

-- First of all we need to find most active users. Those users who have interacted with platform more, would have generated more events and have got higher event count. 
--Therefore we can call them active users. Lets aggregate at event level to get data.

------ QUERY
with user_activity as (
select substring(issue_viewed_at, 1, 10) as interaction_date , end_user_id, user_action from users
), 
------ EXPLANATION: This CTE will be helpful for extracting date from readable date string with timestamp.
user_interaction_count as (
select end_user_id, count(user_action) as user_interaction_count,
min(interaction_date) as min_interaction_date, max(interaction_date) as max_interaction_date
from user_activity group by end_user_id order by count(user_action) desc)
------ EXPLANATION: This CTE contains the business logic. 
-- The idea is if we count number of interaction per end_user_id and the min and max date when first and last interaction happend, we can say when was user was active.
-- The min_interaction_date will indirectly say that when user was onboarded or s/he is a new user. The max_interaction_date will say when did this user had interacted with platform. By looking at the output it is clear that most active users are OLD Users.
select * from user_interaction_count;


/*
(BONUS -  a question from the data analysts) Is there any way we can better store the event attributes to make it easier 
for downstream analysis (particularly for issue create read update delete related events)? 
*/

-- To get what all issues and their count in the table below query is used.
select user_action, count(1) as user_action_count from users where user_action like 'issue%' group by user_action order by count(1) desc;


-- What all attributes are common between all issue releated events can be found using below queries

select event, attributes  from jira_issues where event = 'issuecreated';
/*
issueCreated
{sendMail: 1b4364fe924a8edf298fa9fa0e36c76d, eventTypeName: dbde118c9793cd9958cfdc68033d812f, 
subtasksUpdated: 01282411846f37c536d674deb6ac6806, eventTypeId: 3201351c5b93c382611b102cc65574d4, 
params.eventsource: aa5cc6cd3ae5f27b886ccfb0bfb8e23d, id: 0a7f32dbe7b1a9a11aefb7093f8e0498, 
user.name: a02a070fbccf66dc18af9b9bfadd9847}
*/

select event, attributes  from jira_issues where event = 'issueupdated';
/*
issueUpdated
{changeLog.id: 27b089134b2e11169bbbd7cd04003f71, changeLog.issue: d263c03f0d6e672af6c6c7b3dd3ca1fe, 
sendMail: 1b4364fe924a8edf298fa9fa0e36c76d, eventTypeName: f011fa0cdd89c46e1cec01451064bde7, 
changeLog.created: c2dd3ef1ff8ad444c0105c7cc576a8a5, subtasksUpdated: 1b4364fe924a8edf298fa9fa0e36c76d, 
changeLog.author: df88547f367ba33b9470e085a7bdc738, eventTypeId: bd7c4b48fa5848d205576c13dc30f2d8, 
user.name: df88547f367ba33b9470e085a7bdc738, id: d263c03f0d6e672af6c6c7b3dd3ca1fe, params.eventsource: d68aab1d8dbec4d5231c2c2a5fdd43d6}
*/

select event, attributes  from jira_issues where event = 'issuedeleted';
/*
issueDeleted
{sendMail: 1b4364fe924a8edf298fa9fa0e36c76d, eventTypeName: 87249245d60bdd39c7addf7795969295, 
subtasksUpdated: 01282411846f37c536d674deb6ac6806, eventTypeId: 7f76b990f68b7b6a80452ed978fcc0ab, 
user.name: a23947dc7cffa87fcd16369949dd782b, id: 9ee500e78943e8a34357beaa343a9f99}
*/

/*
Based on common attributes it is clear that they should haven stroed as a separate table with all attributes (with null allowed where not applicable).
The separate table because if we want to run analytics based on which user has created most of issues or average time requires to close the issue after being opened.
For all these things it is better to flatten the structure and keep a separate table.

Other Alternative is store this event attributes as JOSN Column in the database. This will enable DB engine to use JSON functions. 
These are generally very quick compared to text search and error free as well.
*/

-------------------------------------------------- END OF SECTION 3 ---------------------------------------------------------

---------------------------------------- OPTIONAL START OF SECTION 4 --------------------------------------------------------
/*
To Clean the resources that we have created as part of the exercise, RUN following two queries.
*/

-- To Drop source table
drop table jira_issues;

-- To Drop source table
drop table users;