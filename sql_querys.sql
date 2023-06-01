CREATE DATABASE globant;

USE globant;

-- Number of employees hired for each job and department in 2021 divided by quarter. 
-- The table must be ordered alphabetically by department and job.
SELECT
  department,
  job,
  COUNT(CASE WHEN QUARTER(datetime) = 1 THEN name END) AS Q1,
  COUNT(CASE WHEN QUARTER(datetime) = 2 THEN name END) AS Q2,
  COUNT(CASE WHEN QUARTER(datetime) = 3 THEN name END) AS Q3,
  COUNT(CASE WHEN QUARTER(datetime) = 4 THEN name END) AS Q4
FROM hired_employees he 
INNER JOIN departments d ON he.department_id = d.id 
INNER JOIN jobs j ON he.job_id = j.id
WHERE
  YEAR(datetime) = 2021
GROUP BY
	department,
 	job
ORDER BY
  department,
  job;

-- List of ids, name and number of employees hired of each department that hired more employees than 
-- the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).
SELECT 
	d.id, 
	d.department,
	count(he.id) as hired
FROM hired_employees he 
INNER JOIN departments d ON he.department_id = d.id 
INNER JOIN jobs j ON he.job_id = j.id
GROUP BY d.id, d.department
HAVING COUNT(he.id) > (
	SELECT 
		AVG(hired_by_dept)
	FROM (
		SELECT
			department_id,
			count(id) as hired_by_dept
		FROM hired_employees
		WHERE YEAR(datetime) = 2021 
		GROUP BY department_id
	) t
)
ORDER BY hired desc;
