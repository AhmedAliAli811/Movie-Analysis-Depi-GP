use TMDB;
go

/* profit = revenue - budget
   profit margin = ( profit / revenue ) *100

   Weighted voting = (num_votes / (num_votes + m) ) * avg_vote + (m / (num_votes + m) ) * c
   m (threshold or mean of vote count) (minimum number of votes requaired)
   c mean of avg votes
   */

-- for each company compute profit margin , year sales growth --
with HighestGrossing as (
    select c.company_id
        , c.company_name
        , year(m.release_date) as year
        , m.title
        , m.revenue
        , sum(m.budget) over (partition by c.company_name	, year(m.release_date)) as [Total expenses]
        , sum(m.revenue) over (partition by c.company_name, year(m.release_date)) as [Total returns]
        , count(*) over (partition by c.company_name, year(m.release_date) rows between unbounded preceding and unbounded following ) as [Number of films produced]
        , 100.0 * (sum(m.revenue) over (partition by c.company_name, year(m.release_date)) 
                  - sum(m.budget) over (partition by c.company_name, year(m.release_date))) 
                  / nullif(sum(m.revenue) over (partition by c.company_name, year(m.release_date)), 0) as [Profit Margin]
        , row_number() over (partition by c.company_name, year(m.release_date) order by m.revenue desc) as rank
    from movies m join movie_companies mc
	on m.movie_id = mc.movie_id join companies c
	on mc.company_id = c.company_id
    where m.budget >= 200000 and m.revenue > 1000 
        and (c.company_name like '%Marvel%' 
            or c.company_name like '%DC %' 
            or c.company_name like '%Universal%' 
            or c.company_name like '%Sony%' 
            or c.company_name like '%Disney%')
)
select hg.company_name
, hg.year
, hg.[Number of films produced]
, hg.[Total expenses]
, hg.[Total returns]
, hg.[Profit Margin]
, hg.title as [Highest grossing movie]
, 100.0 * hg.revenue / hg.[Total returns] as [Percentage of revenue from highest-grossing movie]
from HighestGrossing hg
where hg.rank = 1 
order by hg.company_name , hg.year 



--The most popular film producing countries and genres in the country--
select country_name , [Total Num of films ] , max_genre as [Most popular Genres]
from (select distinct c.country_name
, count( mc.movie_id) as [Total Num of films ]
, last_value(g.genre_name) over (partition by c.country_name order by avg(m.popularity) rows between unbounded preceding and unbounded following) as max_genre
, last_value(avg(m.popularity)) over (partition by c.country_name order by avg(m.popularity) rows between unbounded preceding and unbounded following) as max_avg_popularity
, row_number() over (partition by c.country_name order by count(distinct m.movie_id) desc ) as rank
from movies m join movie_genres mg on
m.movie_id = mg.movie_id join genres g
on mg.genre_id = g.genre_id join movie_countries mc
on m.movie_id = mc.movie_id join countries c
on mc.country_iso = c.country_iso
group by c.country_iso , c.country_name, g.genre_name
 ) tmp
 where tmp.rank = 1
 order by [Total Num of films ] desc



--Over the past 20 years, Top  actors have occupied the top three positions in terms of film profits and popularity? --> Sql 
with ranked_actors as (
    select a.actor_id
      , a.actor_name
      , sum(m.revenue) - sum(m.budget) as Total_Profit
      , row_number() over (partition by year(m.release_date) order by sum(m.revenue) - sum(m.budget) desc) as profit_rank
    from actors a join movie_actors ma 
	on a.actor_id = ma.actor_id join movies m 
	on ma.movie_id = m.movie_id
    where year( m.release_date ) >= 2000
    group by a.actor_id , a.actor_name , year(m.release_date)
),
top_ranked_actors as (
    select actor_id
      , actor_name
	  , Total_Profit
      , case 
            when profit_rank <= 3 then 1 else 0 
        end as profit_top3 
    from ranked_actors
) 
select actor_id
, actor_name
, sum(profit_top3) as times_in_top_3
, sum(Total_Profit) as [Total returns]
from top_ranked_actors
group by actor_id , actor_name
order by times_in_top_3 desc , [Total returns] desc


--Most Productive Directors

select mcj.crew_id as [Director Id]
, c.crew_name as [Director Name]
, count(*) [Number of films]
, 100.0 * (sum(revenue) - sum(budget)) / nullif( sum(revenue), 0) as [Total profit margin]
from movies m join movie_crew_jobs mcj 
on m.movie_id = mcj.movie_id join crew c
on c.crew_id = mcj.crew_id join jobs j 
on mcj.job_id = j.job_id
where j.job = 'Director' and budget >= 200000 and revenue >= 100000 
group by mcj.crew_id , c.crew_name
order by [Number of films] desc  , [Total profit margin] desc 


--Most Productive Producers

select mcj.crew_id as [Producer Id]
, c.crew_name as [Producer Name]
, avg(budget) as Avg_Budget
, count(*) [Number of produced films]
, 100.0 * (sum(revenue) - sum(budget)) / nullif( sum(revenue), 0) as [Total profit margin]
from movies m join movie_crew_jobs mcj 
on m.movie_id = mcj.movie_id join crew c
on c.crew_id = mcj.crew_id join jobs j 
on mcj.job_id = j.job_id
where j.job = 'producer' and budget >= 200000 and revenue >= 100000 
group by mcj.crew_id , c.crew_name
order by [Number of produced films] desc ,  Avg_Budget desc, [Total profit margin] desc



-- Top Films Rated --
-- Weighted Rating = (num_votes / (num_votes + m) ) * avg_vote + (m / (num_votes + m) ) * avg(avg_votes)

with stats as (
		select 1.0 * avg(vote_count)  as m 
			 , 1.0 *  avg(vote_average) as c	
		from movies
)
select title , 1.0 * vote_average *  (1.0 * vote_count / (vote_count + m )) + c * (m/(vote_count + m)) as [Weighted Rating]
from movies , stats
order by [Weighted Rating] desc



-- Top Actors based on Film Rating
with stats as (
		select 1.0 * avg(vote_count)  as m 
			 , 1.0 *  avg(vote_average) as c	
		from movies
),
movie_ratings as (
    select m.movie_id
      , m.vote_average
      , m.vote_count
      , s.c
      , s.m
      , 1.0 * m.vote_average *  (1.0 * m.vote_count / (m.vote_count + s.m )) + s.c * (s.m/(m.vote_count + s.m)) as weighted_rating
    from  movies m, stats s  
) , actor_weighted_ratings as (
    select a.actor_id
		, a.actor_name
		, count(ma.movie_id) as [number of movies]
		, avg(mr.weighted_rating) as avg_weighted_rating  
    from actors a join movie_actors ma 
	on a.actor_id = ma.actor_id join movie_ratings mr 
	on ma.movie_id = mr.movie_id
    group by a.actor_id, a.actor_name
),
actor_stats as (
    select 1.0 * avg([number of movies]) as m
		, 1.0 * avg(avg_weighted_rating) as c  
    from actor_weighted_ratings
)
select awr.actor_id
, awr.actor_name
, awr.avg_weighted_rating 
, awr.[number of movies]
, s.m
, s.c  
, 1.0 * awr.avg_weighted_rating *  (1.0 * awr.[number of movies] / (awr.[number of movies] + s.m )) + s.c * (s.m/(awr.[number of movies] + s.m)) as weighted_rating

from actor_weighted_ratings awr
, actor_stats s  
where awr.[number of movies] > 5
order by  weighted_rating desc
 


-- Analysis of Movies Featuring Actors with Multiple Roles
with stats as (
		select 1.0 * avg(vote_count)  as m 
			 , 1.0 *  avg(vote_average) as c	
		from movies
) , 
 Movies_Filtered as (
	select distinct m.movie_id
	from movies m join movie_actors ma 
	on m.movie_id = ma.movie_id 
	group by m.movie_id , ma.actor_id
	having count(*) > 1
)
select title 
, 1.0 * vote_average *  (1.0 * vote_count / (vote_count + m )) + c * (m/(vote_count + m)) as [Weighted Rating]
, 100.0 * (m.revenue - m.budget) / nullif(m.revenue, 0) as [Profit Margin]
from stats , movies m join Movies_Filtered mf
on m.movie_id = mf.movie_id
order by [Weighted Rating] desc




-- Duration VS Rating
with stats as (
		select 1.0 * avg(vote_count)  as m 
			 , 1.0 *  avg(vote_average) as c	
		from movies
) ,  movie_ratings as (
    select m.movie_id
	, m.vote_average
	, m.vote_count
	, m.runtime
	, (1.0 * m.vote_average * (1.0 * m.vote_count / (m.vote_count + s.m)))  + (s.c * (s.m / (m.vote_count + s.m))) as weighted_rating
    from movies m , stats s
),

duration_analysis as (
    select 
        case 
            when runtime < 60 then '< 60 minutes'
            when runtime >= 60 and runtime < 90 then '60-90 minutes'
            when runtime >= 90 and runtime < 120 then '90-120 minutes'
            when runtime >= 120 then '> 120 minutes'
        end as duration_category
		, count(m.movie_id) as number_of_movies
		, avg(weighted_rating) as avg_weighted_rating  
    from movie_ratings m
    group by 
        case 
            when runtime < 60 then '< 60 minutes'
            when runtime >= 60 and runtime < 90 then '60-90 minutes'
            when runtime >= 90 and runtime < 120 then '90-120 minutes'
            when runtime >= 120 then '> 120 minutes'
        end
)
select duration_category
, number_of_movies
, avg_weighted_rating
from duration_analysis
order by avg_weighted_rating desc , duration_category



-- how the month of release date affect on Profit Margin
select month(m.release_date) as release_month
     , 100.0 * (sum(revenue) - sum(budget)) / nullif( sum(revenue), 0) as [Total Profit Margin]
    , avg(m.popularity) as avg_popularity
    from movies m
    where m.revenue > 0
    group by month(m.release_date)
	order by [Total Profit Margin] desc , avg_popularity desc