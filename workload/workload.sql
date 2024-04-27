-- Q1 Range query
SELECT * FROM financial.loan WHERE financial.loan.duration = 36;

-- Q2 Range query 2
SELECT * FROM financial.order WHERE financial.order.amount > 10000 and financial.order.amount < 20000 and financial.order.k_symbol = 'LEASING';

-- Q3 Binary join 1 - non-expaanding join
SELECT count(distinct b.account_id) FROM financial.client a, financial.disp b 
where a.client_id = b.client_id and
a.district_id = 18 and b.type = 'DISPONENT';

-- Q4 Binary join 2 - expanding join (group by)
SELECT a.date, count(a.date) FROM financial.account a, financial.trans b 
where a.account_id = b.account_id and 
b.operation = 'VYBER KARTOU' and a.district_id = 18
group by a.date 
order by count(a.date);

-- Q5 3 way join, mixed expanding and non-expanding
SELECT count(distinct a.account_id) FROM financial.account a, financial.trans b, financial.order c
where a.account_id = b.account_id and a.account_id = c.account_id and
b.operation = 'VYBER KARTOU' and a.district_id = 18 and c.k_symbol = 'LEASING';

-- Q6 3 way join, all expanding
SELECT sum(a.amount) FROM financial.order a, financial.trans b, financial.disp c
where a.account_id = b.account_id and b.account_id = c.account_id and
b.operation = 'VYBER KARTOU' and a.k_symbol = 'LEASING';

-- Q7 4 way join I
SELECT min(c.amount) FROM financial.account a, financial.trans b, financial.order c, financial.disp d
where a.account_id = b.account_id and a.account_id = d.account_id and c.account_id = d.account_id and
b.operation = 'VYBER KARTOU' and a.district_id = 18 and c.k_symbol = 'LEASING';

-- Q8 5 way join II
SELECT max(c.amount) FROM financial.account a, financial.trans b, financial.order c, financial.disp d, financial.loan e
where a.account_id = b.account_id and b.account_id = c.account_id and c.account_id = d.account_id and 
b.operation = 'VYBER KARTOU' and c.k_symbol = 'LEASING' and a.district_id = 18 and  e.duration = 36;
