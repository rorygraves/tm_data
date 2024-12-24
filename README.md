Experimental project for exploring TMI historical data.

Data is written to a postgres database for analysis.

Todo:
 *  Create views within code (currently created manually with statements below)
   *  Or manage them as a seperate table updated during data syncs
   * https://stackoverflow.com/questions/32521058/creating-table-view-using-slick
* Resync with TH
* Add district website info:
  * 
* Add extract district info:
  * Website links: https://www.toastmasters.org/Membership/Leadership/district-websites/
  * https://reports2.toastmasters.org/District.cgi?dist=91
    * District location text
    * Rank in world
    * 


---

Views created in Database (created manually)

-- district_summary - latest data for each district

CREATE VIEW district_summary AS
SELECT
month_end_date,
as_of_date,
program_month,
program_year,
region,
district,
dsp,
dec_training,
new_payments,
oct_payments,
april_payments,
late_payments,
charter_payments,
total_ytd_payments,
payment_base,
percent_payment_growth,
paid_club_base,
paid_clubs,
percent_club_growth,
active_clubs,
distinguished_clubs,
select_distinguished_clubs,
presidents_distinguished_clubs,
total_distinguished_clubs,
percent_distinguished_clubs,
drp_status
FROM district_summary_historical
WHERE as_of_date = (SELECT MAX(as_of_date) FROM district_summary_historical);

----

-- club_perf - latest data for each club

CREATE VIEW club_perf AS
SELECT
program_year,
program_month,
month_end_date,
as_of_date,
district,
division,
area,
club_number,
club_name,
club_status,
base_members,
active_members,
goals_met,
level1s,
level2s,
level2s_add,
level3s,
level45dtms,
level45dtms_add,
new_members,
add_new_members,
officers_trained_rd1,
officers_trained_rd2,
cot_met,
members_dues_on_time_oct,
members_dues_on_time_apr,
officer_list_on_time,
goal_10_met,
distinguished_status,
members_growth,
awards_per_member,
dcp_eligibility,
monthly_growth,
members_30_sept,
members_31_mar,
region,
nov_ad_visit,
may_ad_visit,
total_new_members,
late_renewals,
oct_renewals,
apr_renewals,
total_charter,
total_to_date,
charter_suspend_date
FROM club_perf_historical
WHERE as_of_date = (SELECT MAX(as_of_date) FROM club_perf_historical);

-- regions - latest regions for each district

CREATE VIEW regions AS SELECT DISTINCT district, region from district_summary;
