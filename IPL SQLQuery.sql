CREATE DATABASE IPL; --Create Database
USE IPL; --Use Database

CREATE TABLE IPL_Ball --Create Table IPL_Ball
(id int, inning int, over int, ball int, batsman char(50), non_striker char(50), bowler char(50), 
batsman_runs int, extra_runs int, total_runs int, is_wicket int, dismissal_kind char(50), 
player_dismissed char(50), fielder char(50), extras_type char(50), 
batting_team varchar(50), bowling_team varchar(50) );

COPY IPL_Ball --Importing CSV
FROM 'C:\Mohit Agarwal\Desktop\Project 2\IPL Dataset\IPL_Ball.csv' 
DELIMITER ',' CSV HEADER;

SELECT * FROM IPL_Ball; --Display Table IPL_Ball

CREATE TABLE IPL_matches --Create Table IPL_matches 
(id int, city char(50), match_date varchar(50), player_of_match char(50), venue varchar(255), 
neutral_venue varchar(255), team1 varchar(100), team2 varchar(100), toss_winner varchar(100), 
toss_decision char(50), winner varchar(100), result varchar(50), result_margin int, 
eliminator char(10), method varchar(50), umpire1 char(50), umpire2 char(50) );

COPY IPL_matches --Importing CSV
FROM 'C:\Mohit Agarwal\Desktop\Project 2\IPL Dataset\IPL_matches.csv' 
DELIMITER ',' CSV HEADER;

SELECT * FROM IPL_matches;

/*Your first priority is to get 2-3 players with high S.R who have faced at least 500 balls.And
to do that you have to make a list of 10 players you want to bid in the auction so that
when you try to grab them in auction you should not pay the amount greater than you
have in the purse for a particular player. */

CREATE TABLE Top_Batsmen_StrikeRate (
    batsman NVARCHAR(255),
    strike_rate DECIMAL(4,1),
    player_rank INT
);

INSERT INTO Top_Batsmen_StrikeRate (batsman, strike_rate, player_rank)
SELECT batsman, 
       CAST(strike_rate AS DECIMAL(4,1)), 
       DENSE_RANK() OVER (ORDER BY strike_rate DESC) AS player_rank
FROM (
    SELECT batsman, 
           CAST(player_total_runs AS FLOAT) / balls_faced * 100 AS strike_rate
    FROM (
        SELECT batsman, 
               SUM(batsman_runs) AS player_total_runs, 
               COUNT(ball) AS balls_faced
        FROM IPL_Ball 
        WHERE NOT extras_type = 'wides'
        GROUP BY batsman
    ) AS a 
    WHERE balls_faced > 500
) AS b 
ORDER BY strike_rate DESC 
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

SELECT * FROM Top_Batsmen_StrikeRate;

/*Now you need to get 2-3 players with good Average who have played more than 2 ipl
seasons. And to do that you have to make a list of 10 players you want to bid in the
auction so that when you try to grab them in auction you should not pay the amount
greater than you have in the purse for a particular player.*/

CREATE TABLE Top_Batsmen_Average (
    batsman NVARCHAR(255),
    player_average DECIMAL(10,2),
    player_rank INT
);

INSERT INTO Top_Batsmen_Average (batsman, player_average, player_rank)
SELECT TOP 10 batsman, player_average, 
       DENSE_RANK() OVER (ORDER BY player_average DESC) AS player_rank
FROM (
    SELECT batsman, 
           CAST(total_runs AS FLOAT) / NULLIF(dismissed_no, 0) AS player_average
    FROM (
        SELECT batsman, 
               SUM(batsman_runs) AS total_runs,
               SUM(is_wicket) AS dismissed_no,
               COUNT(DISTINCT YEAR(TRY_CONVERT(DATE, date, 105))) AS played_years
        FROM (
            SELECT a.batsman, 
                   a.batsman_runs, 
                   CAST(a.is_wicket AS INT) AS is_wicket, 
                   b.date
            FROM IPL_Ball AS a 
            FULL JOIN IPL_matches AS b 
            ON a.id = b.id
        ) AS c 
        GROUP BY batsman
    ) AS d  
    WHERE dismissed_no >= 1 AND played_years > 2 
) AS e 
ORDER BY player_average DESC;

SELECT * FROM Top_Batsmen_Average;

/*Now you need to get 2-3 Hard-hitting players who have scored most runs in boundaries
and have played more the 2 ipl season. To do that you have to make a list of 10 players
you want to bid in the auction so that when you try to grab them in auction you should
not pay the amount greater than you have in the purse for a particular player.*/

CREATE TABLE Top_Batsmen_BoundaryPercentage (
    batsman NVARCHAR(255),
    boundary_percentage DECIMAL(3,1),
    player_rank INT
);

INSERT INTO Top_Batsmen_BoundaryPercentage (batsman, boundary_percentage, player_rank)
SELECT TOP 10 batsman, 
       CAST(boundary_percentage AS DECIMAL(3,1)) AS boundary_percentage, 
       DENSE_RANK() OVER (ORDER BY boundary_percentage DESC) AS player_rank
FROM (
    SELECT *, 
           (CAST(boundary_runs AS FLOAT) / NULLIF(total_runs, 0) * 100) AS boundary_percentage
    FROM (
        SELECT batsman, 
               total_runs, 
               SUM(batsman_runs) AS boundary_runs, 
               COUNT(batsman_runs) AS boundaries_total, 
               COUNT(DISTINCT YEAR(TRY_CONVERT(DATE, date, 105))) AS played_years
        FROM (
            SELECT a.batsman, 
                   a.batsman_runs, 
                   SUM(a.batsman_runs) OVER (PARTITION BY a.batsman) AS total_runs, 
                   b.date 
            FROM IPL_Ball AS a 
            FULL JOIN IPL_matches AS b 
            ON a.id = b.id
        ) AS c 
        WHERE batsman_runs = 4 OR batsman_runs = 6 
        GROUP BY total_runs, batsman
    ) AS d 
    WHERE played_years > 2 
) AS e 
ORDER BY boundary_percentage DESC;

SELECT * FROM Top_Batsmen_BoundaryPercentage;

/*Your first priority is to get 2-3 bowlers with good economy who have bowled at least 500
balls in IPL so far.To do that you have to make a list of 10 players you want to bid in the
auction so that when you try to grab them in auction you should not pay the amount
greater than you have in the purse for a particular player.*/

CREATE TABLE Top_Bowlers_Economy (
    bowler NVARCHAR(255),
    economy DECIMAL(3,1),
    bowler_rank INT
);

INSERT INTO Top_Bowlers_Economy (bowler, economy, bowler_rank)
SELECT TOP 10 
       bowler, 
       CAST(economy AS DECIMAL(3,1)) AS economy, 
       DENSE_RANK() OVER (ORDER BY economy ASC) AS bowler_rank
FROM (
    SELECT *, 
           CAST(conceded_runs AS FLOAT) / NULLIF(bowled_overs, 0) AS economy
    FROM (
        SELECT bowler, 
               conceded_runs, 
               bowled_balls, 
               CAST(COUNT(ball) / 6 AS INT) + 
               CAST((COUNT(ball) % 6) / 10.0 AS FLOAT) AS bowled_overs
        FROM (
            SELECT bowler, 
                   ball, 
                   SUM(total_runs) OVER (PARTITION BY bowler) AS conceded_runs, 
                   COUNT(ball) OVER (PARTITION BY bowler) AS bowled_balls
            FROM IPL_Ball
        ) AS a 
        WHERE bowled_balls > 500 
        GROUP BY bowler, conceded_runs, bowled_balls
    ) AS b 
) AS c 
ORDER BY economy ASC;

SELECT * FROM Top_Bowlers_Economy;

/*Now you need to get 2-3 bowlers with the best strike rate and who have bowled at least
500 balls in IPL so far.To do that you have to make a list of 10 players you want to bid in
the auction so that when you try to grab them in auction you should not pay the amount
greater than you have in the purse for a particular player.*/

CREATE TABLE Top_Bowlers_StrikeRate (
    bowler NVARCHAR(255),
    strike_rate DECIMAL(5,1),
    bowler_rank INT
);

INSERT INTO Top_Bowlers_StrikeRate (bowler, strike_rate, bowler_rank)
SELECT TOP 10 
       bowler, 
       CAST(strike_rate AS DECIMAL(5,1)) AS strike_rate, 
       DENSE_RANK() OVER (ORDER BY strike_rate) AS bowler_rank
FROM (
    SELECT *, 
           CAST(total_balls AS FLOAT) / NULLIF(wicket_taken, 0) AS strike_rate 
    FROM (
        SELECT bowler, 
               total_balls, 
               SUM(CAST(is_wicket AS INT)) AS wicket_taken 
        FROM (
            SELECT bowler, 
                   is_wicket, 
                   COUNT(ball) OVER (PARTITION BY bowler) AS total_balls 
            FROM IPL_Ball
        ) AS a 
        WHERE is_wicket > 0 AND total_balls > 500 
        GROUP BY bowler, total_balls
    ) AS b
) AS c
ORDER BY strike_rate asc;

SELECT * FROM Top_Bowlers_StrikeRate;

/*Now you need to get 2-3 All_rounders with the best batting as well as bowling strike rate
and who have faced at least 500 balls in IPL so far and have bowled minimum 300
balls.To do that you have to make a list of 10 players you want to bid in the auction so
that when you try to grab them in auction you should not pay the amount greater than
you have in the purse for a particular player.*/

CREATE TABLE AllRounders (
    all_rounder NVARCHAR(255),
    batting_strike_rate DECIMAL(5,2),
    bowling_strike_rate DECIMAL(5,2),
    player_rank INT
);

INSERT INTO AllRounders (all_rounder, batting_strike_rate, bowling_strike_rate, player_rank)
SELECT TOP 10 batsman AS all_rounder, 
       CAST(batting_strike_rate AS DECIMAL(5,2)) AS batting_strike_rate, 
       CAST(bowling_strike_rate AS DECIMAL(5,2)) AS bowling_strike_rate, 
       DENSE_RANK() OVER (ORDER BY batting_strike_rate DESC, bowling_strike_rate ASC) AS player_rank
FROM (
    SELECT b.batsman, 
           (CAST(b.total_runs AS FLOAT) / NULLIF(b.balls_faced, 0)) * 100 AS batting_strike_rate,
           CAST(p.balls_bowled AS FLOAT) / NULLIF(p.total_wickets, 0) AS bowling_strike_rate
    FROM (
        SELECT batsman, 
               SUM(batsman_runs) AS total_runs, 
               COUNT(*) AS balls_faced
        FROM IPL_Ball
        GROUP BY batsman
        HAVING COUNT(*) >= 500
    ) AS b
    INNER JOIN (
        SELECT bowler, 
               COUNT(*) AS balls_bowled, 
               SUM(CAST(is_wicket AS INT)) AS total_wickets
        FROM IPL_Ball
        GROUP BY bowler
        HAVING COUNT(*) >= 300 AND SUM(CAST(is_wicket AS INT)) > 0
    ) AS p
    ON b.batsman = p.bowler
) AS e
ORDER BY batting_strike_rate DESC, bowling_strike_rate ASC;

SELECT * FROM AllRounders;

/*Wicketkeeper
Should have been played more than 2 IPL seasons.

Having batting strike rate of 125+.

Having bowling economy rate < 10.

Having good fielding rate.
*/

CREATE TABLE Wicketkeeper (
    player VARCHAR(50),
    seasons_played INT,
    batting_strike_rate FLOAT,
    bowling_economy FLOAT
);

INSERT INTO Wicketkeeper(player, seasons_played, batting_strike_rate, bowling_economy)
SELECT TOP 10
    p.player,
    p.seasons_played,
    p.batting_strike_rate,
    b.bowling_economy
FROM (
    -- Batting Stats
    SELECT 
        batsman AS player,
        COUNT(DISTINCT YEAR(m.date)) AS seasons_played,
        SUM(batsman_runs) AS total_batsman_runs,
        COUNT(*) AS balls_faced,
        (SUM(batsman_runs) * 100.0 / NULLIF(COUNT(*), 0)) AS batting_strike_rate
    FROM IPL_Ball b
    JOIN IPL_matches m ON b.id = m.id
    GROUP BY batsman
) p
LEFT JOIN (
    -- Bowling Stats
    SELECT 
        bowler AS player,
        SUM(total_runs - extra_runs) AS runs_conceded,
        COUNT(*) / 6.0 AS overs_bowled,
        (SUM(total_runs - extra_runs) / NULLIF(COUNT(*) / 6.0, 0)) AS bowling_economy
    FROM IPL_Ball
    GROUP BY bowler
) b ON p.player = b.player
LEFT JOIN (
    -- Fielding Stats
    SELECT 
        fielder AS player
    FROM IPL_Ball
    WHERE is_wicket = 1 AND fielder IS NOT NULL
    GROUP BY fielder
) f ON p.player = f.player
WHERE p.seasons_played > 2
    AND p.batting_strike_rate >= 125
    AND (b.overs_bowled >= 1 AND b.bowling_economy < 10);
   
SELECT * FROM Wicketkeeper;

--Deliveries is the table created using the IPL_Ball data whereas the Matches table has been created using the IPL_Matches data

CREATE TABLE Deliveries 
(id int, inning int,[over] int, ball int, batsman char(50), non_striker char(50), bowler char(50), 
batsman_runs int, extra_runs int, total_runs int, is_wicket int, dismissal_kind char(50), 
player_dismissed char(50), fielder char(50), extras_type char(50), 
batting_team varchar(50), bowling_team varchar(50) );

COPY Deliveries --Importing CSV
FROM 'C:\Mohit Agarwal\Desktop\Project 2\IPL Dataset\IPL_Ball.csv' 
DELIMITER ',' CSV HEADER;

Select * FROM Deliveries;

CREATE TABLE Matches 
(id int, city char(50), match_date varchar(50), player_of_match char(50), venue varchar(255), 
neutral_venue varchar(255), team1 varchar(100), team2 varchar(100), toss_winner varchar(100), 
toss_decision char(50), winner varchar(100), result varchar(50), result_margin int, 
eliminator char(10), method varchar(50), umpire1 char(50), umpire2 char(50) );

COPY Matches --Importing CSV
FROM 'C:\Mohit Agarwal\Desktop\Project 2\IPL Dataset\IPL_matches.csv' 
DELIMITER ',' CSV HEADER;

Select * FROM Matches;

/*1. Get the count of cities that have hosted an IPL match*/

SELECT COUNT(CITY) FROM Matches AS city;

/*2. Create table deliveries_v02 with all the columns of the table ‘deliveries’ and an additional
column ball_result containing values boundary, dot or other depending on the total_run
(boundary for >= 4, dot for 0 and other for any other number)
(Hint 1 : CASE WHEN statement is used to get condition based results)
(Hint 2: To convert the output data of the select statement into a table, you can use a
subquery. Create table table_name as [entire select statement].*/

create table deliveries_v02 as ( select *, 
case 
    when total_runs >= 4 then 'boundary' 
    when total_runs = 0 then 'dot' 
    else 'other' 
end ball_result 
from Deliveries );

/*3. Write a query to fetch the total number of boundaries and dot balls from the deliveries_v02 table.*/

select ball_result, count(ball_result) AS count_balls 
from deliveries_v02 
where ball_result='boundary' or ball_result='dot' 
group by ball_result;

/*4. Write a query to fetch the total number of boundaries scored by each team from the
deliveries_v02 table and order it in descending order of the number of boundaries
scored.*/

select batting_team, count(ball_result) as no_of_boundaries 
from deliveries_v02 
where ball_result='boundary' 
group by batting_team 
order by no_of_boundaries desc;

/*5. Write a query to fetch the total number of dot balls bowled by each team and order it in
descending order of the total number of dot balls bowled.*/

select bowling_team,count(ball_result) as no_of_dot_balls from deliveries_v02 where 
ball_result='dot' group by bowling_team order by no_of_dot_balls desc;

/*6. Write a query to fetch the total number of dismissals by dismissal kinds where dismissal kind is not NA*/

select dismissal_kind, count(dismissal_kind) AS count_dismissal from deliveries_v02 where not dismissal_kind='NA' 
group by dismissal_kind;

/*7. Write a query to get the top 5 bowlers who conceded maximum extra runs from the deliveries table*/

SELECT bowler, 
       SUM(extra_runs) AS conceded_extra_runs 
FROM deliveries_v02  
GROUP BY bowler 
ORDER BY conceded_extra_runs DESC 
OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;


/*8. Write a query to create a table named deliveries_v03 with all the columns of
deliveries_v02 table and two additional column (named venue and match_date) of venue
and date from table matches*/

SELECT a.*, b.venue, b.match_date  
INTO deliveries_v03  
FROM deliveries_v02 AS a  
FULL JOIN Matches AS b  
ON a.id = b.id;

select * from deliveries_v03;

/*9. Write a query to fetch the total runs scored for each venue and order it in the descending order of total runs scored.*/

select venue,sum(total_runs) as total_runs from deliveries_v03 
group by venue order by venue desc;

/*10. Write a query to fetch the year-wise total runs scored at Eden Gardens and order it in the descending order of total runs scored.*/

SELECT YEAR(match_date) AS year, SUM(total_runs) AS total_runs  
FROM deliveries_v03  
WHERE venue = 'Eden Gardens'  
GROUP BY YEAR(match_date)  
ORDER BY total_runs DESC;









