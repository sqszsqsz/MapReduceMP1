MapReduce_MP1

A simple implementation of MapReduce to find the click events from different users that are close to each other. 

The required logic can be represented as the following SQL.

SELECT a.ymdh, a.user_id, b.user_id
FROM   data a, data b
WHERE  a.click = 1 AND b.click = 1
AND    a.user_id != null AND b.user_id != null
AND    a.user_id < b.user_id
AND    abs(TIMESTAMPDIFF(SECOND, a.ymdh, b.ymdh)) < 2;
