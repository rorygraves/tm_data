Experimental project for exploring TMI historical data.

// open endpoint for TI advanced search
//    https://www.toastmasters.org/api/sitecore/FindAClub/Search?q=&district=91&advanced=1&latitude=1&longitude=1

//  postmasters he is run from July to June, so 2021-2022 runs from July 2021 to June 2022
//

https://dashboards.toastmasters.org/export.aspx?type=CSV&report=districtsummary~10/31/2024~~2024-2025

// current year
// https://dashboards.toastmasters.org/?id=21&month=8
// onchange="dll_onchange(this.value,&#39;districtsummary~11/30/2022~12/9/2022~2022-2023&#39;, this)">

// https://dashboards.toastmasters.org/2023-2024/?id=21&month=3
// onchange="dll_onchange(this.value,"districtsummary~3/31/2024~4/10/2024~2023-2024", this)">
// https://dashboards.toastmasters.org/2023-2024/export.aspx?type=CSV&report=districtsummary~9/30/2023~10/12/2023~2023-2024

// https://dashboards.toastmasters.org/export.aspx?type=CSV&report=districtsummary~9/30/2024~~2024-2025

//    https://dashboards.toastmasters.org/2023-2024/export.aspx?type=CSV&report=districtsummary~11/30/2023~30/6/20232023-2024
// this year
// https://dashboards.toastmasters.org/?id=21&month=8

// previous years
// https://dashboards.toastmasters.org/2024-2025/?id=21&month=3


//    https://commons.apache.org/proper/commons-csv/user-guide.html#Header_auto_detection
//
//      https://www.toastmasters.org/api/sitecore/FindAClub/Search?q=&district=91&advanced=1&latitude=1&longitude=1


grant select on public.club_perf_historical to tm_data_florian;
grant select on public.district_summary_historical to tm_data_florian;
grant select on public.club_perf to tm_data_rory;



# Data Issues

## Duplicate rows:
e.g. D04 2012-7 Club data 

TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,0D,0A,1225069,Mobile Toasters,Active,0,7,7,0.0,true,0,ClubDCPData(2012,7,2012-07-31,1225069,0,0,0,0,0,0,0,0,0,0,0,0,false,false,0,0,false,false,false,0,0),,0,7,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,0,true,false,7,1,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,7,0,0,7,7,)))
TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,0D,0A,1225069,Mobile Toasters,Active,22,7,-15,0.0,false,1,ClubDCPData(2012,7,2012-07-31,1225069,0,0,0,0,0,0,0,0,0,0,0,0,true,true,0,0,false,false,true,0,0),,0,7,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,0,true,false,7,1,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,7,0,0,7,7,)))
TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,E,02,1225069,Mobile Toasters,Active,0,7,7,0.0,true,0,ClubDCPData(2012,7,2012-07-31,1225069,0,0,0,0,0,0,0,0,0,0,0,0,false,false,0,0,false,false,false,0,0),,0,7,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,0,true,false,7,1,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,7,0,0,7,7,)))
TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,E,02,1225069,Mobile Toasters,Active,22,7,-15,0.0,false,1,ClubDCPData(2012,7,2012-07-31,1225069,0,0,0,0,0,0,0,0,0,0,0,0,true,true,0,0,false,false,true,0,0),,0,7,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,0,true,false,7,1,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,E,02,1225069,Mobile Toasters,0,7,0,0,7,7,)))

TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,0D,0A,2492534,Brocade,Active,0,20,20,0.0,true,0,ClubDCPData(2012,7,2012-07-31,2492534,0,0,0,0,0,0,0,0,0,0,0,0,false,false,0,0,false,false,false,0,0),,0,20,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,true,false,20,0,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,0,0,0,20,Charter 07/09/12)))
TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,0D,0A,2492534,Brocade,Active,37,20,-17,0.0,true,0,ClubDCPData(2012,7,2012-07-31,2492534,0,0,0,0,0,0,0,0,0,0,0,0,false,false,0,0,false,false,true,0,0),,0,20,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,true,false,20,0,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,0,0,0,20,Charter 07/09/12)))
TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,F,02,2492534,Brocade Communicators,Active,0,20,20,0.0,true,0,ClubDCPData(2012,7,2012-07-31,2492534,0,0,0,0,0,0,0,0,0,0,0,0,false,false,0,0,false,false,false,0,0),,0,20,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,true,false,20,0,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,0,0,0,20,Charter 07/09/12)))
TMClubDataPoint(2012,7,2012-07-31,2012-07-31,04,02,F,02,2492534,Brocade Communicators,Active,37,20,-17,0.0,true,0,ClubDCPData(2012,7,2012-07-31,2492534,0,0,0,0,0,0,0,0,0,0,0,0,false,false,0,0,false,false,true,0,0),,0,20,0,Some(TMDivClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,true,false,20,0,)),Some(TMDistClubDataPoint(2012,7,2012-07-31,04,F,02,2492534,Brocade Communicators,0,0,0,0,0,20,Charter 07/09/12)))

Solved by taking last value of the duplicates.

---
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
