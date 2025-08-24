use project;
select * from walmartsales;


-- CLEANING THE DATASET
-- Checking the data type of columns to correct it for cleaning the data
describe walmartsales;


-- CREATING CLEANED TABLE
create table cleaned_walmartdata as 
select `Invoice ID`, Branch, City, `Customer type`, Gender, `Product line`,
 cast(`Unit price` AS DECIMAL(10,2)) AS Unit_price, Quantity, 
 cast(`Tax 5%` AS DECIMAL(10,2)) AS `Tax_5%`, 
 cast(Total AS DECIMAL(10,2)) AS Total, 
 str_to_date(`Date`, '%d-%m-%Y') as sale_date, 
 monthname(str_to_date(`Date`, '%d-%m-%Y')) as Months, 
 cast(`Time` as time) as sale_time, Payment, 
 cast(Cogs as decimal(10,2)) as Cogs, 
 cast(`gross margin percentage` as decimal(10,2)) as gross_margin_percentage, 
 cast(`gross income` as decimal(10,2)) as gross_income, 
 cast(Rating as decimal(10,2)) as Rating, 
 `Customer ID` FROM walmartsales;
SELECT * FROM cleaned_walmartdata;


-- CHEKING FOR NULL VALUES
select * from cleaned_walmartdata where 
`Invoice ID` is null or Branch is null or City is null or
 `Unit_price` is null or Quantity is null;
 
 
-- CHECKING FOR DUPLICATE VALUES IN INVOICE ID
select `Invoice ID`, count(*) from cleaned_walmartdata
 group by `Invoice ID` HAVING count(*)>1;
 
 
-- TRIMING TEXT COLUMNS
update cleaned_walmartdata set
`Invoice ID`=trim(`Invoice ID`),
Branch= trim(Branch),
City= trim(City),
`Customer type`= trim(`Customer type`),
Gender= trim(Gender),
`Product line`= trim(`Product line`),
Months= trim(Months),
Payment= trim(Payment);


-- Task 1: identifying the Top Branch by Sales Growth Rate 
-- calculated total_sales for each month and branch in CTE monthly_sales 
with monthly_sales as 
(select Branch, date_format(sale_date, '%m-%Y') as Month_number, 
Months, sum(total) as Total_sales 
from cleaned_walmartdata group by branch, month_number, MONTHS),
-- calculated previous month sales using lag in CTE monthly_previous_sales
monthly_previous_sales as 
(select Branch, Month_number, Months, Total_sales, 
lag(Total_sales) over (partition by Branch order by Month_number) as previous_sales 
from monthly_sales),
/* Calculated growth rate % by using formula 
((total_sales-previous sales)/previous sales*100 in CTE growth_rate */
growth_rate as 
(select Branch, Month_number, Months, Total_sales, previous_sales, 
round(((Total_sales-previous_sales)/previous_sales)*100, 2) as `Growth_rate_%` 
from monthly_previous_sales)
/* Calculated average growth rate for each branch by not including null values
(as jan has no previous month sales) to find the top performing Branch */
select Branch, round(avg(`Growth_rate_%`),2) as Average_Growth_rate 
from growth_rate 
where `Growth_rate_%` is not null 
group by Branch order by Average_Growth_rate desc;
    
    
-- Task 2: Finding the Most Profitable Product Line for Each Branch
/* Calculated the most profitable product line for each branch 
by summing gross income for each product line per branch 
and ranked by highest profit using rank() and extracted only top 1 rank in each branch using CTE */ 
with product_profit as 
(select Branch, `Product line`, sum(gross_income) as Profit, 
rank() over (partition by Branch order by sum(gross_income) desc) as rnk 
from cleaned_walmartdata group by Branch, `Product line`)
select Branch, `Product line`, Profit from product_profit where rnk=1;



-- Task 3: Analyzing Customer Segmentation Based on Spending 
/* Based on total purchase amount the customer segmentation has been done as High, Medium, and Low */
with customer_purchase_behaviour as 
(select `Customer ID`, sum(Total) AS Total_amount 
from cleaned_walmartdata 
group by `Customer ID` order by `Customer ID`)
select `Customer ID`, Total_amount, 
case when Total_amount>=23000 then "High" 
when Total_amount between 20000 and 22999 then "Medium" 
else "Low" end as Customer_segmentation from customer_purchase_behaviour;



-- Task 4: Detecting Anomalies in Sales Transactions
with avg_sale as 
(select `Invoice ID`, Branch, `Product line`, Total, 
avg(Total) over(partition by `Product line`) as avg_sales from cleaned_walmartdata)
select `Invoice ID`, Branch, `Product line`, Total, avg_sales, 
case when Total> (avg_sales * 2) then 'High Anomaly' 
when Total<(avg_sales * 0.5) then 'Low Anomaly' 
else 'Normal' end as Anomaly_detection from avg_sale;


-- Task 5: Most Popular Payment Method by City 
/*used CTE, first calculated count of payment by grouping city and payment, 
then used window function for ranking, then extracted only records that ranking first, 
by desc the count of payment for each city*/
with popular_payment as 
(select City, Payment, count(Payment) as `No_of_payment` 
from cleaned_walmartdata group by city, Payment), 
ranking as 
(select city, Payment, `No_of_payment`, 
rank() over(partition by city order by `No_of_payment` desc) AS Rnk 
from popular_payment)
select city, Payment, `No_of_payment` from ranking where rnk=1;


-- Task 6: Monthly Sales Distribution by Gender 
/* calculated total sales by female and male month wise by grouping
 also calculated percentage of sale contribution by gender using CTE */
with monthly_gender as 
(select date_format(sale_date, '%Y-%m') as `Date`, Months, Gender, 
sum(Total) as Total_sale from cleaned_walmartdata 
group by Months, gender, `Date` order by `Date`, gender)
select *, round(Total_sale*100/sum(total_sale) over(partition by `Date`),0) as Percentage 
from monthly_gender;

-- Task 7: Best Product Line by Customer Type
/* calculated total sale of product line by customer type 
and ranked product line based on each customer type. 
Extracted top 3 product lines prefered by different customer type.*/
with product_by_customer as 
(select `Customer type`, `Product line`, sum(Total) as Total_sale 
from cleaned_walmartdata 
group by `Product line`, `Customer type` 
order by `Customer type`), 
ranking as 
(select `Customer type`, `Product line`, Total_sale, 
rank() over (partition by `Customer type` order by Total_sale desc) as Rnk 
from product_by_customer)
select `Customer type`, `Product line`, Total_sale, Rnk 
from ranking WHERE rnk between 1 and 3;

-- Task 8: Identifying Repeat Customers 
/*Used CTE and pivioting data to calculate number of puchases 
done by each customers within 30 days time frame*/
with min_date_cte as 
(select min(sale_date) as min_date 
from cleaned_walmartdata), 
`second` as 
(select date_add(min_date, INTERVAL 31 DAY) as second_start_date 
from min_date_cte), 
third as 
(select date_add(second_start_date, INTERVAL 31 DAY) as Third_start_date 
from `second`)
select `Customer ID`,
COUNT(case when sale_date between min_date and date_add(min_date, INTERVAL 30 DAY)  
THEN `Invoice ID` END) AS 1st_30days_interval_Purchases, 
COUNT(case when sale_date between second_start_date and date_add(second_start_date, INTERVAL 30 DAY)  
THEN `Invoice ID` END) AS 2nd_30days_interval_Purchases, 
COUNT(case when sale_date between Third_start_date and date_add(Third_start_date, INTERVAL 30 DAY)  
THEN `Invoice ID` END) AS 3rd_30days_interval_Purchases
 from cleaned_walmartdata, min_date_cte, `second`, third group by `Customer ID` order by `Customer ID`;
 
 -- Task 9: Finding Top 5 Customers by Sales Volume 
 /* Calculated top 5 customers who have generated the most sales Revenue 
 using CTE and windows function*/
 
 with customer_sales as 
 (select `Customer ID`, sum(Total) as Total_sale 
 from cleaned_walmartdata 
 group by `customer ID`), 
 ranking as 
 (select `Customer ID`, Total_sale, 
 rank() over (order by Total_sale desc) as customers_rank 
 from customer_sales)
 select * from ranking where customers_rank<=5;
 
 -- Task 10: Analyzing Sales Trends by Day of the Week 
 /* calculated total sales for each day of the week 
 by grouping by days and ranking from highest sales*/
select dayname(sale_date) as Days, 
sum(Total) as Total_sales, 
rank() over (order by sum(Total) desc) as Ranking_days 
from cleaned_walmartdata group by days;


