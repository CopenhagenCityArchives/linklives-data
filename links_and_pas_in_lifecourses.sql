SELECT
	LC.life_course_id,
	COUNT(*) as link_count,
	group_concat(L.link_id, ', ') AS link_ids,
	group_concat(L.pa_id, ', ') AS pa_ids,
	group_concat(L.source_id, ', ') AS source_ids
FROM Links L
LEFT JOIN Life_courses LC ON L.link_id = LC.link_id
GROUP BY LC.life_course_id
ORDER BY link_count DESC;