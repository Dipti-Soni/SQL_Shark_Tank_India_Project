create database if not exists sharktank;
use sharktank;

load data infile 'D:/sharktank.csv'
into table sharktank
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows;

-- 1 Your Team must promote shark Tank India season 4, A senior comes up with the idea to show highest funding domain wise
-- so that new startups can be attracted, and you were assigned the task to show the same.
select * from 
(select Industry, Total_Deal_Amount_in_lakhs, 
ROW_NUMBER() over(PARTITION BY industry order by Total_Deal_Amount_in_lakhs desc) as 'ranking' from sharktank)t 
where ranking = 1;

-- 2. You have been assigned the role of finding the domain where female as pitchers have female to male pitcher ratio >70%
select industry, round(sum(female_presenters)/sum(male_presenters)*100,2) as ratio from sharktank GROUP BY industry
having ratio > 70;

-- 3. You are working at marketing firm of Shark Tank India, you have got the task to determine volume of per season sale pitch
-- made, pitches who received offer and pitches that were converted. Also show the percentage of pitches converted and percentage
-- of pitches entertained.
select season_number, received_offer, accepted_offer, count(pitch_number) from sharktank
group by season_number, received_offer, accepted_offer
having received_offer = 'Yes';

select a.season_number,total, r_offer, a_offer, r_offer/total*100, a_offer/total*100 from 
(select season_number, count(pitch_number) as 'total' from sharktank group by season_number) a 
inner join
(select season_number, received_offer, count(pitch_number) as 'r_offer' from sharktank where received_offer = 'Yes' group by season_number) b
on a.season_number = b.season_number
inner join 
(select season_number, accepted_offer, count(pitch_number) as 'a_offer' from sharktank where accepted_offer = 'Yes' group by season_number) c
on b.season_number = c.season_number ;

-- 4. As a venture capital firm specializing in investing in startups featured on a renowned entrepreneurship TV show, you are 
-- determining the season with the highest average monthly sales and identify the top 5 industries with the highest average monthly 
-- sales during that season to optimize investment decisions?
select * from 
(select *, row_number() over(partition by season_number) as 'rnk' from 
(select season_number, industry, avg(monthly_sales_in_lakhs) as 'avg_sales' from sharktank
GROUP BY season_number, industry order by season_number, avg_sales desc) t1 )t2 
where rnk <6;

-- another solution
set @seas= (select season_number  from
(
select  season_number , round(avg(monthly_sales_in_lakhs),2)as 'average' from sharktank where monthly_sales_in_lakhs!= 'Not_mentioned'
 group by season_number  
 )k order by average desc
 limit 1);

select industry , round(avg(monthly_sales_in_lakhs),2) as average from  sharktank where season_number = @seas and monthly_sales_in_lakhs!= 'Not_mentioned'
group by industry
order by average desc
limit 5;

-- 5.As a data scientist at our firm, your role involves solving real-world challenges like identifying industries with consistent increase in funds raised 
-- over multiple seasons. This requires focusing on industries where data is available across all three years. Once these industries are pinpointed, your task 
-- is to delve into the specifics, analyzing the number of pitches made, offers received, and offers converted per season within each industry.
select industry, season_number, total, row_number() over(partition by industry) as 'rnk' from 
(select industry, season_number, sum(total_deal_amount_in_lakhs) as 'total' from sharktank
group by industry, season_number order by industry, season_number) t ;

-- changing the above table pivot/unpivot
with validindustries as (
select industry, 
sum(case when season_number = 1 then total_deal_amount_in_lakhs end) as season1, 
sum(case when season_number = 2 then total_deal_amount_in_lakhs end) as season2,
sum(case when season_number = 3 then total_deal_amount_in_lakhs end) as season3
from sharktank
group by industry having season3 > season2 and season2 > season1 and season1 != 0)

select v.industry, s.season_number,
count(s.startup_name) as 'no_of_startups', count(case when s.Received_Offer='Yes' then Received_Offer end) as 'received',
count(case when s.Accepted_Offer='Yes' then Accepted_Offer end) as 'accepted'
from sharktank s join validindustries v where s.industry = v.industry
group by industry, season_number;

-- 6. Every shark wants to know in how many years their investment will be returned, so you must create a system for them, where shark will enter the name of 
-- startup and based on the total deal and equity given in how many years their principal amount will be returned and make their investment decision worth.
select Startup_Name,
	case
		when Accepted_Offer = 'No' or Accepted_Offer = 'No Offer Received.' then 'offer not accepted or not received'
		when Accepted_Offer = 'Yes' and (Yearly_Revenue_in_lakhs = '' or Yearly_Revenue_in_lakhs = 0) then 'previous data missing' 
        else round(Total_Deal_Amount_in_lakhs/((Yearly_Revenue_in_lakhs*Total_Deal_Amount_in_lakhs)/100),2)
	end as 'result'
from sharktank;
select * from sharktank;

-- putting the above query in a stored procedure
delimiter //
create procedure principal_returned (in startup varchar(100))
begin
	select Startup_Name,
		case
			when Accepted_Offer = 'No' or Accepted_Offer = 'No Offer Received.' then 'offer not accepted or not received'
			when Accepted_Offer = 'Yes' and (Yearly_Revenue_in_lakhs = '' or Yearly_Revenue_in_lakhs = 0) then 'previous data missing' 
			else round(Total_Deal_Amount_in_lakhs/((Yearly_Revenue_in_lakhs*Total_Deal_Amount_in_lakhs)/100),2)
		end as 'result'
	from sharktank where Startup_Name = startup;
end
// delimiter ;

call principal_returned('BluePineFoods');

-- 7. In the world of startup investing, we're curious to know which big-name investor, often referred to as "sharks," tends to put the most money into each 
-- deal on average. This comparison helps us see who's the most generous with their investments and how they measure up against their fellow investors.
select sharkname, round(avg(investment),2)  as 'average' from
(
SELECT `Namita_Investment_Amount_in lakhs` AS investment, 'Namita' AS sharkname FROM sharktank WHERE `Namita_Investment_Amount_in lakhs` > 0
union all
SELECT `Vineeta_Investment_Amount_in_lakhs` AS investment, 'Vineeta' AS sharkname FROM sharktank WHERE `Vineeta_Investment_Amount_in_lakhs` > 0
union all
SELECT `Anupam_Investment_Amount_in_lakhs` AS investment, 'Anupam' AS sharkname FROM sharktank WHERE `Anupam_Investment_Amount_in_lakhs` > 0
union all
SELECT `Aman_Investment_Amount_in_lakhs` AS investment, 'Aman' AS sharkname FROM sharktank WHERE `Aman_Investment_Amount_in_lakhs` > 0
union all
SELECT `Peyush_Investment_Amount_in_lakhs` AS investment, 'peyush' AS sharkname FROM sharktank WHERE `Peyush_Investment_Amount_in_lakhs` > 0
union all
SELECT `Amit_Investment_Amount_in_lakhs` AS investment, 'Amit' AS sharkname FROM sharktank WHERE `Amit_Investment_Amount_in_lakhs` > 0
union all
SELECT `Ashneer_Investment_Amount` AS investment, 'Ashneer' AS sharkname FROM sharktank WHERE `Ashneer_Investment_Amount` > 0
)k group by sharkname;

-- 8. Develop a stored procedure that accepts inputs for the season number and the name of a shark. The procedure will then provide detailed insights into 
-- the total investment made by that specific shark across different industries during the specified season. Additionally, it will calculate the percentage 
-- of their investment in each sector relative to the total investment in that year, giving a comprehensive understanding of the shark's investment 
-- distribution and impact.
select season_number, 'Namita' as sharkname, industry, sum(`Namita_Investment_Amount_in lakhs`) as 'amt_inv', 
(100*sum(`Namita_Investment_Amount_in lakhs`)) / (select sum(`Namita_Investment_Amount_in lakhs`) from sharktank where season_number = 1) as 'percentage_of_inv'
from sharktank where season_number = 1 group by industry;

delimiter //
create procedure sharks_investment (in sea_num int, in sharkname varchar(10))
begin
	case
		when sharkname ='Namita' then 
			set @total = (select sum(`Namita_Investment_Amount_in lakhs`) from sharktank where season_number = sea_num);
			select industry, sum(`Namita_Investment_Amount_in lakhs`) as 'amt_inv', (100*sum(`Namita_Investment_Amount_in lakhs`)) / @total as 'percentage_of_inv'
			from sharktank where season_number = sea_num group by industry;
        when sharkname ='Vineeta' then 
			set @total = (select sum(`Vineeta_Investment_Amount_in_lakhs`) from sharktank where season_number = sea_num);
			select industry, sum(`Vineeta_Investment_Amount_in_lakhs`) as 'amt_inv', (100*sum(`Vineeta_Investment_Amount_in_lakhs`)) / @total as 'percentage_of_inv'
			from sharktank where season_number = sea_num group by industry;
        when sharkname ='Anupam' then 
			set @total = (select sum(`Anupam_Investment_Amount_in_lakhs`) from sharktank where season_number = sea_num);
			select industry, sum(`Anupam_Investment_Amount_in_lakhs`) as 'amt_inv', (100*sum(`Anupam_Investment_Amount_in_lakhs`)) / @total as 'percentage_of_inv'
			from sharktank where season_number = sea_num group by industry;
	end case;
end
//
delimiter ;
call sharks_investment(2, 'Anupam');