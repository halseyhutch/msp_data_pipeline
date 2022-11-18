UPDATE hospital_quality
SET quality_rating = %(quality_rating)s
WHERE hospital_pk = %(hospital_pk)s AND record_date = %(record_date)s;