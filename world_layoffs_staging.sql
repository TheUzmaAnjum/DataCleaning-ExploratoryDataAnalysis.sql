---- SQL Project - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

select *
FROM world_layoffs.layoffs;
--'world_layoffs' is the database I created and as I will be working under 'world_layoffs' database itself, I will not be specifying it repeatedly.
-- first thing I did is creating a staging table. This is the one to work in and clean the data. There should always be a table with the raw data.

create table layoffs_staging
like layoffs;

-- Inserting the values into a new table from the raw table
Insert layoffs_staging
select * 
from layoffs;
-- Check it out
select * 
from layoffs_staging;

-- Now,when we perform data cleaning we usually follow a few steps
-- 1. Checking and removing the duplicates
-- 2. Standardize data and fix errors
-- 3. Look at null values and see what can be done
-- 4. Remove any columns and rows that are not necessary - few ways

-- 1.REMOVE DUPLICATES

-- First let's check for duplicates

select * 
from layoffs_staging;

--Here We really need to look at every single row to be accurate to find out real duplicates with WINDOW function

select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) as row_num
from layoffs_staging;

-- these are the ones we want to delete where the row number is > 1 or 2 
-- now you may want to write it like this using CTE:

with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

--Check it out
select * 
from layoffs_staging
where company= 'Casper';


with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;
--One solution would be to create a new column and add those row numbers into that column. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!
--Creating a new column

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Check it out

select * 
from layoffs_staging2;
-- Insert the data into the new table with new column 'row_num'

Insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) as row_num
from layoffs_staging;

--Now Check for duplicates where row_num > 1 and then delete it

select * 
from layoffs_staging2
where row_num > 1;

delete 
from layoffs_staging2
where row_num > 1;


---2.STANDARDIZING THE DATA AND FIXING ERRORS

select * 
from layoffs_staging2;

---- if we look at industry it looks like we have some null and empty rows, let's take a look at these

select distinct(industry)
from layoffs_staging2
order by industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- nothing wrong here

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
-----------------------------------------------
-- I also noticed the Crypto industry has multiple different variations. Now lets standardize that - let's say all to Crypto

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'; 

---- now that's taken care of:

select distinct(country)
from layoffs_staging2
order by industry;
--------------------------------
-- Lets take a look now 

SELECT *
FROM layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.

select distinct(country), trim(trailing '.' from country)
from layoffs_staging2
order by industry;

update layoffs_staging2
set country= trim(trailing '.' from country)
where country like 'United States%';
-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

---- Let's also fix the date columns:

select `date`
from layoffs_staging2;

-- During our examination initially we noticed that date was a text column so now lets change the datatype of date ,we can use 'str_to_date' to update this field

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
alter table layoffs_staging2
modify column `date` date;

--DONE..Check it out

SELECT *
FROM layoffs_staging2;

--3.LOOKING FOR NULL VALUES

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. REMOVING UNNECESSARY COLUMNS

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Delete Useless data we can't really use

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
 
select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

-- HERE'S OUR TABLE WITH ALL DATA CLEANED
SELECT * 
FROM layoffs_staging2;
