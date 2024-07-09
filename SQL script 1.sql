--Count athletes by country

Select Country,COUNT(PersonName) as Total_athletes
from Athletes
group by Country
Order by Total_athletes DESC;

-- total medals won by each country

Select Team_Country,sum(Gold) as Total_Gold,sum(Silver)as Total_Silver,sum(Bronze) as Total_Bronze
from Medals
group by Team_Country
order by Total_Gold desc;

-- Calculate the average number of entries by gender for each discipline
Select Discipline,avg(Female),avg(male) 
from Gender
Group by Discipline;