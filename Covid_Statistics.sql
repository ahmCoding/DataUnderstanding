-- what is the data about ? 
select top 1000 * from Covid..CovidData
/* the rows ,
location ,date,population, new_cases, new deaths , new_vaccination ,median_age,male_smokers,
female_smokers, human_development_index,population_density 

could be interessting for us
 */

-- nr of rows in the table 
select count(date) from Covid..CovidData



-- information about continen and income level
select top 3 continent,location,date,median_age,extreme_poverty,new_vaccinations ,population,new_deaths,new_cases,male_smokers,female_smokers
,population_density,human_development_index
from Covid..CovidData
WHERE continent is  null and location like('%income%')

select top 3 continent,location,date,median_age,extreme_poverty,new_vaccinations ,population,new_deaths,new_cases,male_smokers,female_smokers
,population_density,human_development_index
from Covid..CovidData
WHERE continent is  null and location not like('%income%')

-- final selection
select continent,location,date,median_age,extreme_poverty,new_vaccinations ,population,new_deaths,new_cases,male_smokers,female_smokers
,population_density,human_development_index,
count(location) over(PARTITION by date ) as NrofEntriesinThisDate
from Covid..CovidData
WHERE continent is not null and  location not like('%income%')


-- create temp table 
CREATE TABLE #CountryInfos(
    continent VARCHAR(50),
    location VARCHAR(50),
    date Date,
    population FLOAT,
    new_cases FLOAT,
    new_deaths FLOAT,
    new_vaccinations FLOAT,
    extreme_poverty FLOAT,
    male_smokers FLOAT,
    female_smokers FLOAT,
    population_density FLOAT,
    human_development_index FLOAT,
);

-- CTE for selecting the needed information
With tst as (
select continent,location,date,population,new_cases,new_deaths,new_vaccinations,extreme_poverty,male_smokers,female_smokers
,population_density,human_development_index
from Covid..CovidData
WHERE continent is not null and location not like('%income%')
)
Insert into #CountryInfos(continent,location,date,population,new_cases,new_deaths,new_vaccinations,extreme_poverty,male_smokers,female_smokers
,population_density,human_development_index)
select * from tst;

-- controlling the results
select  * from #CountryInfos

-- location without key information 
select location from #CountryInfos
GROUP BY location 
having sum (new_cases) is  Null OR sum(new_deaths) is  Null

-- deleting the locations without key information 
DELETE from  #CountryInfos 
where location in (select  location from #CountryInfos
GROUP BY location 
having sum (new_cases) is  Null OR sum(new_deaths) is  Null)


--cumulative numbers
WITH cum_calc as(
select continent,location,population,CAST(human_development_index as decimal(5,2))as human_development_index,
CAST(extreme_poverty as decimal(5,2))as extreme_poverty,CAST(male_smokers as decimal(5,2))as male_smokers,
CAST(female_smokers as decimal(5,2)) as female_smokers,CAST(population_density as decimal(7,2))population_density,
 convert(DECIMAL(4,2), (sum(new_cases) / NULLIF(max(population),0) * 100)) as Infected_Percentage_of_Population,
 convert(DECIMAL(4,2), (sum(new_deaths)/ NULLIF(sum(new_cases),0) * 100)) as Died_Percentage_of_Infected,
 sum(new_deaths) as count_of_deaths, 
 sum(new_cases) as count_of_infectetion,
convert(decimal(5,2),(sum(new_deaths) /  sum(new_cases) - 1.0 ) * -100) as recovered_percentage
from #CountryInfos
GROUP BY continent,location,population,human_development_index,extreme_poverty,male_smokers,female_smokers,population_density
)
select * from cum_calc
where  Infected_Percentage_of_Population <> 0 and Died_Percentage_of_Infected <> 0 
order by count_of_deaths desc

-- day by day calcultions 
select continent,location,population, date ,
sum(new_cases) OVER(partition by location order by date) as sum_of_infected_people,
sum(new_deaths) OVER(partition by location order by date) as sum_of_died_people,
convert(DECIMAL(8,2),((sum(new_deaths)OVER(partition by location order by date))/
 Nullif(sum(new_cases)OVER(partition by location order by date),0)* 100))
 as deadly_percentage_of_infection,
convert(DECIMAL(8,2),((sum(new_vaccinations)OVER(partition by location order by date))/
 Nullif(Max(population)OVER(partition by location order by date),0)* 100))
 as vaccinated_percentage_of_population,
 sum(new_vaccinations) OVER(partition by location order by date) as sum_of_new_vaccinations,
 CAST(human_development_index as decimal(5,2))as human_development_index,
 CAST(extreme_poverty as decimal(5,2))as extreme_poverty,CAST(male_smokers as decimal(5,2))as male_smokers,
 CAST(female_smokers as decimal(5,2)) as female_smokers,CAST(population_density as decimal(7,2))population_density
from #CountryInfos
where new_deaths<> 0 or  new_cases<> 0 or  new_vaccinations<> 0 
ORDER by location,date