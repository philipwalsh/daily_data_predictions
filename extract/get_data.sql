use [database name here]
go


BEGIN

-- create a temp table variable for the known data
-- insert the existing data
-- create a table variable for the missing data
-- impute the missing data
-- union the known with the misisng for the complete set
-- but dont send back 4th july holiday data
-- and exclude the week before and the week of christmas
-- once you get the result set, 
--  1) check for -1 in the hour, minute and record count.  if you find any, then the script is failing to impute the mean.  Investigate.
--  2) if data looks good,
--     a) open excel 
--     b) select all from results grid, copy with headers
--     c) paste, then while the cells are still highlighted, format cells as text and paste again
--     d) export, change file type to csv, save as ./data/2018.csv

DECLARE @data_arrival TABLE (
    arrived_date datetime NOT NULL,
    arrived_hour int NOT NULL,
    arrived_minute int NOT NULL,
	record_count int NOT NULL)


DECLARE @missing_arrival TABLE (
    arrived_date datetime NOT NULL,
    arrived_hour int NOT NULL,
    arrived_minute int NOT NULL,
	record_count int NOT NULL)



	insert into @data_arrival
	select
		Notify_Date as [arrived_date],  
		datepart(hh,notify_date) as [arrived_hour],
		datepart(mi,notify_date) as [arrived_minute],
		Record_Count as [record_count]
	from 
		[table name here]
	where 
		notify_date >= '2017-12-25' and notify_date<'2019-01-04'
		-- grabbing a little before and a little after what i really want for now
		-- then i will trim it down later
		-- select datepart(dw,convert(datetime,'2018-01-07')) -- 1
		-- select datepart(dw,convert(datetime,'2018-12-29')) -- 7



	-- loop through and find the missing days

	-- table variable example
	-- https://www.sqlservertutorial.net/sql-server-user-defined-functions/sql-server-table-variables/
	-- cursor example
	-- https://docs.microsoft.com/en-us/sql/t-sql/language-elements/declare-cursor-transact-sql?view=sql-server-ver15

	SET NOCOUNT ON;  
  
	DECLARE @c_arrived_date datetime
	DECLARE @c_arrived_hour int  
	DECLARE @c_arrived_minute int
	DECLARE @c_record_count int  
   
	declare @last_date datetime
	declare @expected_date datetime
	declare @days_missing int

	DECLARE @Iteration INT
	SET @Iteration = 1


	declare @avg_record_count int
	declare @avg_hour int
	declare @avg_minute int

	-- set them all to -1, if i ever see a -1 in the data
	-- then i know i have to investigate
	set @avg_record_count = -1
	set @avg_hour = -1
	set @avg_minute = -1



	DECLARE arrival_cursor CURSOR FOR   
	SELECT 
		arrived_date ,
		arrived_hour ,
		arrived_minute ,
		record_count 
	FROM @data_arrival  
	ORDER BY arrived_date asc
  
	OPEN arrival_cursor  
  
	FETCH NEXT FROM arrival_cursor
	INTO @c_arrived_date , @c_arrived_hour , @c_arrived_minute , @c_record_count  
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN
		if @last_date is null
		begin
			print 'init'
			set @last_date = @c_arrived_date
		end
		else
		begin
			set @expected_date = @last_date + 1
			-- we are in the meat of the loop
			set @days_missing = abs(datediff(d, @c_arrived_date, @expected_date))
			if @days_missing > 0
			begin
				print 'missing: ' + convert(varchar,@last_date+1) + ' span: ' + convert(varchar, @days_missing)
			end
			

			set @Iteration=1
			WHILE @Iteration <= @days_missing
			BEGIN
				-- get the mean of the previous records
				set @avg_record_count = (select avg(record_count) from @data_arrival where arrived_date < (@last_date+@Iteration) and datepart(dw,arrived_date) = datepart(dw, (@last_date+@Iteration)))
				set @avg_hour = (select avg(arrived_hour) from @data_arrival where arrived_date < (@last_date+@Iteration) and datepart(dw,arrived_date) = datepart(dw, (@last_date+@Iteration)))
				set @avg_minute = (select avg(arrived_minute) from @data_arrival where arrived_date < (@last_date+@Iteration) and datepart(dw,arrived_date) = datepart(dw, (@last_date+@Iteration)))


				insert into @missing_arrival (arrived_date, arrived_hour, arrived_minute , record_count)
				values(@last_date+@Iteration,@avg_hour,@avg_minute, @avg_record_count)
				
				SET @Iteration = @Iteration + 1
				
				-- failsafe breakout incase i have some logic bug
				if @Iteration > 100
					break;
			END
			
			set @last_date = @c_arrived_date

		end


		FETCH NEXT FROM arrival_cursor
		INTO @c_arrived_date , @c_arrived_hour , @c_arrived_minute , @c_record_count  
	END   
	CLOSE arrival_cursor;  
	DEALLOCATE arrival_cursor; 


	/*
	
	-- need to elimiate 4th of july week
	select datepart(dw,convert(datetime, '2018-07-01')), datepart(dw,convert(datetime, '2018-07-04')), datepart(dw,convert(datetime, '2018-07-07'))
	-- and eliminate christmas week and the week leading up to christmas week
	select datepart(dw,convert(datetime, '2018-12-15'))

	*/


	select * 
	from @data_arrival 
	where (arrived_date > '2018-01-07' and arrived_date < '2018-07-01')
	or (arrived_date > '2018-07-08' and arrived_date < '2018-12-16')
	union all
	select * 
	from @missing_arrival 
	where (arrived_date > '2018-01-07' and arrived_date < '2018-07-01')
	or (arrived_date > '2018-07-08' and arrived_date < '2018-12-16')
	order by arrived_date asc
END