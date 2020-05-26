SELECT t.name,
         t.hour,
         t.highest,
         i.ts
FROM 
    (SELECT name,
         hour,
         max(high) AS highest
    FROM 
        (SELECT name,
         high,
         ts,
         substr(ts,
        12,
        2) AS hour
        FROM "23")
        GROUP BY  (name,hour)) AS t
    INNER JOIN 
    (SELECT name,
        high,
        ts,
        substr(ts,
        12,
        2) AS hour
    FROM "23") AS i
    ON t.highest=i.high
        AND t.hour=i.hour
        AND t.name=i.name
ORDER BY  (name,hour)