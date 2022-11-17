UPDATE hospital_beds
SET all_adult_hospital_beds_7_day_avg = %(all_adult_hospital_beds_7_day_avg)s,
all_pediatric_inpatient_beds_7_day_avg = %(all_pediatric_inpatient_beds_7_day_avg)s,
all_adult_hospital_inpatient_bed_occupied_7_day_coverage = %(all_adult_hospital_inpatient_bed_occupied_7_day_coverage)s,
all_pediatric_inpatient_bed_occupied_7_day_avg = %(all_pediatric_inpatient_bed_occupied_7_day_avg)s,
total_icu_beds_7_day_avg = %(total_icu_beds_7_day_avg)s,
icu_beds_used_7_day_avg = %(icu_beds_used_7_day_avg)s,
inpatient_beds_used_covid_7_day_avg = %(inpatient_beds_used_covid_7_day_avg)s,
staffed_icu_adult_patients_confirmed_covid_7_day_avg = %(staffed_icu_adult_patients_confirmed_covid_7_day_avg)s
WHERE hospital_pk = %(hospital_pk)s AND collection_week = %(collection_week)s;