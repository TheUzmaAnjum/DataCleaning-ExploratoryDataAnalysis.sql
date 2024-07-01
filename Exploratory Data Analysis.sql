-- Exploratory data analysis
-- Here we are going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!

select *
from layoffs_staging2;

--BASIC QUERIES

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funds_raised_millions we can see how big some of these companies were

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi!..WOW! It raised like 2 billion dollars and went under


-- INTERMEDIATE QUERIES AND MOSTLY USING GROUP BY--
-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY total_laid_off DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc
limit 10;

--by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset


select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select `date`, sum(total_laid_off)
from layoffs_staging2
group by date;

select Year(`date`), sum(total_laid_off)
from layoffs_staging2
group by Year(`date`)
order by 1 desc;

select Year(`date`), avg(percentage_laid_off)
from layoffs_staging2
group by Year(`date`)
order by 1 desc;

select substring(`date`,1,7) as `month` , sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ;

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

---ADVANCED QUERIES---
-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at 

WITH Company_Year  AS 
(
select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(`date`)
),
  Company_Year_Rank as
(
select *, dense_rank() OVER(PARTITION BY years order by total_laid_off desc) as ranking
from Company_year
where years is not null
)
select company, years, total_laid_off, ranking
from Company_Year_Rank
where ranking <= 5
order by years asc, total_laid_off desc;

----ROLLING TOTAL OF LAYOFFS PER MONTH
--Here, we get sum of total layoffs per month
select substring(`date`,1,7) as `month` , sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ;

----But we want to see rolling total of layoffs in each year,
---So with the execution of below query, we get month and year, total layoffs in each consecutive month and also
---rolling total for each year.
with Rolling_Total as
(
select substring(`date`,1,7) as `month` , sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 
)
select `month`, total_off, sum(total_off)
over(order by `month`) as rolling_total
from Rolling_Total ;


select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;
-----------------
