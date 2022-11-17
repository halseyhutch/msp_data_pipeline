UPDATE hospital_quality
SET record_date = %(record_date)s,
quality_rating = %(quality_rating)s,
WHERE hospital_pk = %(hospital_pk)s;