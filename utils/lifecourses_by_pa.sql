SELECT
	L.pa_id,
	L.source_id,
	COUNT(*) as life_course_count,
	group_concat(LC.life_course_id, ', ') AS life_course_ids
FROM Links L
LEFT JOIN Life_courses LC ON L.link_id = LC.link_id
GROUP BY L.pa_id, L.source_id
ORDER BY life_course_count DESC;