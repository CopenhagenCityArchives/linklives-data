SELECT
	LC.life_course_id,
	L.*,
	coalesce(C45.navn, '') || coalesce(C50.navn, '') || coalesce(C60.navn, '') as name,
	coalesce(C45.køn, '') || coalesce(C50.køn, '') || coalesce(C60.køn, '') as sex,
	coalesce(C45.alder, '') || coalesce(C50.alder, '') || coalesce(C60.alder, '') as age,
	coalesce(C45.fødested, '') || coalesce(C50.fødested, '') || coalesce(C60.fødested, '') as birthplace,
	coalesce(C45.Sogne, '') || coalesce(C50.Sogne, '') || coalesce(C60.Sogne, '') as parish,
	coalesce(C45.Amt, '') || coalesce(C50.Amt, '') || coalesce(C60.Amt, '') as county,
	coalesce(C45.Herred, '') || coalesce(C50.Herred, '') || coalesce(C60.Herred, '') as district,
	coalesce(C45.name_clean, '') || coalesce(C50.name_clean, '') || coalesce(C60.name_clean, '') as name_clean,
	coalesce(C45.age_clean, '') || coalesce(C50.age_clean, '') || coalesce(C60.age_clean, '') as age_clean,
	coalesce(C45.gender_clean, '') || coalesce(C50.gender_clean, '') || coalesce(C60.gender_clean, '') as sex_clean,
	coalesce(C45.birth_place_clean, '') || coalesce(C50.birth_place_clean, '') || coalesce(C60.birth_place_clean, '') as birth_place_clean,
	coalesce(C45.name_std, '') || coalesce(C50.name_std, '') || coalesce(C60.name_std, '') as name_std,
	coalesce(C45.first_name_std, '') || coalesce(C50.first_name_std, '') || coalesce(C60.first_name_std, '') as firstnames_std,
	coalesce(C45.last_name_std, '') || coalesce(C50.last_name_std, '') || coalesce(C60.last_name_std, '') as surnames_std,
	coalesce(C45.county_std, '') || coalesce(C50.county_std, '') || coalesce(C60.county_std, '') as county_std,
	coalesce(C45.parish_std, '') || coalesce(C50.parish_std, '') || coalesce(C60.parish_std, '') as parish_std,
	coalesce(C45.other_std, '') || coalesce(C50.other_std, '') || coalesce(C60.other_std, '') as other_std,
	coalesce(C45.district_std, '') || coalesce(C50.district_std, '') || coalesce(C60.district_std, '') as district_std
FROM Links L
LEFT JOIN Life_courses LC ON L.link_id = LC.link_id
LEFT JOIN census_1845 C45 ON C45.pa_id = L.pa_id AND L.source_id = 2
LEFT JOIN census_1850 C50 ON C50.pa_id = L.pa_id AND L.source_id = 1
LEFT JOIN census_1860 C60 ON C60.pa_id = L.pa_id AND L.source_id = 0
ORDER BY LC.life_course_id, L.link_id;