/*
Lab 1 report <Alejo Pérez-Martynas Lukosevicius and alepe026-marlu207>
*/

/* All non code should be within SQL-comments like this */ 


/*
Drop all user created tables that have been created when solving the lab
*/

DROP TABLE IF EXISTS new_item CASCADE;


/* Have the source scripts in the file so it is easy to recreate!*/

SOURCE company_schema.sql;
SOURCE company_data.sql;

/*
Question 1
*/
SELECT 
    *
FROM
    jbemployee;
/*   
+------+--------------------+--------+---------+-----------+-----------+
| id   | name               | salary | manager | birthyear | startyear |
+------+--------------------+--------+---------+-----------+-----------+
|   10 | Ross, Stanley      |  15908 |     199 |      1927 |      1945 |
|   11 | Ross, Stuart       |  12067 |    NULL |      1931 |      1932 |
|   13 | Edwards, Peter     |   9000 |     199 |      1928 |      1958 |
|   26 | Thompson, Bob      |  13000 |     199 |      1930 |      1970 |
|   32 | Smythe, Carol      |   9050 |     199 |      1929 |      1967 |
|   33 | Hayes, Evelyn      |  10100 |     199 |      1931 |      1963 |
|   35 | Evans, Michael     |   5000 |      32 |      1952 |      1974 |
|   37 | Raveen, Lemont     |  11985 |      26 |      1950 |      1974 |
|   55 | James, Mary        |  12000 |     199 |      1920 |      1969 |
|   98 | Williams, Judy     |   9000 |     199 |      1935 |      1969 |
|  129 | Thomas, Tom        |  10000 |     199 |      1941 |      1962 |
|
*/
/*
Question 2
*/
SELECT 
    name
FROM
    jbdept
ORDER BY name;
/*
+------------------+
| name             |
+------------------+
| Bargain          |
| Book             |
| Candy            |
| Children's       |
| Children's       |
| Furniture        |
| Giftwrap         |
| Jewelry          |
| Junior Miss      |
| Junior's         |
| Linens           |
| Major Appliances |
| Men's            |
| Sportswear       |
| Stationary       |
| Toys             |
| Women's          |
| Women's          |
| Women's          |
+------------------+

*/
/*
Question 3
*/
SELECT 
    *
FROM
    jbparts
WHERE
    qoh = 0;
/*
+----+-------------------+-------+--------+------+
| id | name              | color | weight | qoh  |
+----+-------------------+-------+--------+------+
| 11 | card reader       | gray  |    327 |    0 |
| 12 | card punch        | gray  |    427 |    0 |
| 13 | paper tape reader | black |    107 |    0 |
| 14 | paper tape punch  | black |    147 |    0 |
+----+-------------------+-------+--------+------+

*/
/*
Question 4
*/
SELECT 
    name
FROM
    jbemployee
WHERE
    salary BETWEEN 9000 AND 10000;
/*
+----------------+
| name           |
+----------------+
| Edwards, Peter |
| Smythe, Carol  |
| Williams, Judy |
| Thomas, Tom    |
+----------------+

*/
/*
Question 5
*/
SELECT 
    name, startyear - birthyear AS age
FROM
    jbemployee;
/*
+--------------------+------+
| name               | age  |
+--------------------+------+
| Ross, Stanley      |   18 |
| Ross, Stuart       |    1 |
| Edwards, Peter     |   30 |
| Thompson, Bob      |   40 |
| Smythe, Carol      |   38 |
| Hayes, Evelyn      |   32 |
| Evans, Michael     |   22 |
| Raveen, Lemont     |   24 |
| James, Mary        |   49 |
| Williams, Judy     |   34 |
| Thomas, Tom        |   21 |
| Jones, Tim         |   20 |
| Bullock, J.D.      |    0 |
| Collins, Joanne    |   21 |
| Brunet, Paul C.    |   21 |
| Schmidt, Herman    |   20 |
| Iwano, Masahiro    |   26 |
| Smith, Paul        |   21 |
| Onstad, Richard    |   19 |
| Zugnoni, Arthur A. |   21 |
| Choy, Wanda        |   23 |
| Wallace, Maggie J. |   19 |
| Bailey, Chas M.    |   19 |
| Bono, Sonny        |   24 |
| Schwarz, Jason B.  |   15 |
+--------------------+------+

*/
/*
Question 6
*/
SELECT 
    name
FROM
    jbemployee
WHERE
    NAME LIKE '%son,%';
/*
+---------------+
| name          |
+---------------+
| Thompson, Bob |
+---------------+

*/
/*
Question 7
*/
SELECT 
    name
FROM
    jbitem
WHERE
    supplier IN (SELECT 
            id
        FROM
            jbsupplier
        WHERE
            name = 'Fisher-Price');
/*
+-----------------+
| name            |
+-----------------+
| Maze            |
| The 'Feel' Book |
| Squeeze Ball    |
+-----------------+

*/
/*
Question 8
*/
SELECT 
    it.name
FROM
    jbitem it,
    jbsupplier sup
WHERE
    it.supplier = sup.id
        AND sup.name LIKE 'Fish%';
/*
+-----------------+
| name            |
+-----------------+
| Maze            |
| The 'Feel' Book |
| Squeeze Ball    |
+-----------------+

*/
/*
Question 9
*/
SELECT 
    name
FROM
    jbcity
WHERE
    id IN (SELECT 
            city
        FROM
            jbsupplier);
/*
+----------------+
| name           |
+----------------+
| Amherst        |
| Boston         |
| New York       |
| White Plains   |
| Hickville      |
| Atlanta        |
| Madison        |
| Paxton         |
| Dallas         |
| Denver         |
| Salt Lake City |
| Los Angeles    |
| San Diego      |
| San Francisco  |
| Seattle        |
+----------------+

*/
/*
Question 10
*/
SELECT 
    name, color
FROM
    jbparts
WHERE
    weight >= (SELECT 
            weight
        FROM
            jbparts
        WHERE
            name = 'card reader');
/*
+--------------+--------+
| name         | color  |
+--------------+--------+
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card reader  | gray   |
| card punch   | gray   |
+--------------+--------+
*/
/*
Question 11
*/
SELECT 
    t1.name, t1.color
FROM
    jbparts t1
        JOIN
    jbparts t2 ON t1.weight >= t2.weight
        AND t2.name = 'card reader';
/*
+--------------+--------+
| name         | color  |
+--------------+--------+
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card reader  | gray   |
| card punch   | gray   |
+--------------+--------+

*/
/*
Question 12
*/        
SELECT 
    AVG(weight)
FROM
    jbparts
WHERE
    color = 'black';
/*
+-------------+
| avg(weight) |
+-------------+
|    347.2500 |
+-------------+

*/
/*
Question 13
*/     
SELECT 
    sup.name, SUM(suply.quan * parts.weight) total_weight
FROM
    jbsupply suply,
    jbsupplier sup,
    jbcity city,
    jbparts parts
WHERE
    suply.supplier = sup.id
        AND sup.city = city.id
        AND suply.part = parts.id
        AND city.state = 'Mass'
GROUP BY sup.name;

/*
+--------------+--------------+
| name         | total_weight |
+--------------+--------------+
| DEC          |         3120 |
| Fisher-Price |      1135000 |
+--------------+--------------+
2 rows in set (0.00 sec)
*/
/*
Question 14
*/     
CREATE TABLE new_item (
    id INT,
    name VARCHAR(20),
    dept INT NOT NULL,
    price INT,
    qoh INT UNSIGNED,
    supplier INT NOT NULL,
    CONSTRAINT pk_new_item PRIMARY KEY (id),
    CONSTRAINT fk_new_item_dept FOREIGN KEY (dept)
        REFERENCES jbdept (id),
    CONSTRAINT fk_new_item_supplier FOREIGN KEY (supplier)
        REFERENCES jbsupplier (id)
);
INSERT INTO new_item
select * from jbitem where 
price > (select avg(price) from jbitem);
/*
Query OK, 0 rows affected, 1 warning (0.02 sec)
Query OK, 6 rows affected (0.00 sec)
Records: 6  Duplicates: 0  Warnings: 0
*/