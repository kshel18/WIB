import sqlite3
import datetime
import pandas as pd
import numpy as np
import random

con = sqlite3.connect('db')

def SELECT(sql):
  return pd.read_sql(sql, con)

def user_table():    
    userId = [x for x in range(1, 1001)]
    age = [np.random.randint(12, 60) for _ in range(1000)]
    users = pd.DataFrame({'userId': userId, 'age': age})
    return users

def purchases_table():
    purchaseId = [x for x in range(1, 10001)]
    userId = [random.choice([x for x in range(1, 1001)]) for _ in range(10000)]
    itemId = [random.choice([x for x in range(10, 41)]) for _ in range(10000)]
    date = pd.to_datetime([datetime.datetime(random.randint(2021, 2022), random.randint(1, 12), np.random.randint(1, 29)).\
            strftime("%d/%m/%y") for _ in range(10000)])
    purchases = pd.DataFrame({'purchaseId': purchaseId, 'userId': userId,
                             'itemId': itemId, 'date': date})
    return purchases

def items_table():
    itemId = pd.Series([x for x in range(10, 40)]).unique()
    price = [np.random.choice([x for x in range(100, 5500, 300)]) for _ in range(len(itemId))]
    items = pd.DataFrame({'itemId': itemId, 'price': price})
    return items

def load_table(table):
    table.to_sql(f'{table}', con=con, index=False, if_exists='replace')
    
def load_tables():
    Users = user_table()
    Purchases = purchases_table()
    Items = items_table()

    for table in (Users, Purchases, Items):
        load_table(table)
        
query1 = \
'''
SELECT AVG(price_per_month) AS mean_month_price_per_user
FROM (
    SELECT u.userId, SUM(i.price) / COUNT(STRFTIME('%m', p.date)) AS price_per_month
    FROM Purchases p
    LEFT JOIN Users u ON p.userId = u.userId
    LEFT JOIN Items i ON p.itemId = i.itemId
    WHERE u.age BETWEEN 18 AND 25
    GROUP BY u.userId
    )
'''

query2 = \
'''
SELECT AVG(price_per_month) AS mean_month_price_per_user
FROM (
    SELECT u.userId, SUM(i.price) / COUNT(STRFTIME('%m', p.date)) AS price_per_month
    FROM Purchases p
    LEFT JOIN Users u ON p.userId = u.userId
    LEFT JOIN Items i ON p.itemId = i.itemId
    WHERE u.age BETWEEN 26 AND 35
    GROUP BY u.userId
    )
'''

query3 = \
'''
SELECT year, month, MAX(price) AS max_price
FROM (
    SELECT STRFTIME('%Y', p.date) AS year, STRFTIME('%m', p.date) AS month, SUM(i.price) AS price
    FROM Purchases p
    LEFT JOIN Users u ON p.userId = u.userId
    LEFT JOIN Items i ON p.itemId = i.itemId
    WHERE u.age > 35
    GROUP BY STRFTIME('%y', p.date), STRFTIME('%m', p.date)
    )
GROUP BY year
'''

query4 = \
'''
SELECT p.itemId, SUM(i.price) AS price
FROM Purchases p
LEFT JOIN Items i ON p.itemId = i.itemId
WHERE STRFTIME('%Y', p.date) = '2022'
GROUP BY p.itemId
ORDER BY price DESC
LIMIT 1
'''

query5 = \
'''
SELECT p.itemId, SUM(i.price) AS price, (SUM(i.price) * 1.0 / SUM(SUM(i.price)) OVER()) AS percent
FROM Purchases p
LEFT JOIN Items i ON p.itemId = i.itemId
GROUP BY p.itemId
ORDER BY percent DESC
LIMIT 3
'''

querys = [query1, query2, query3, query4, query5]

if __name__ == '__main__':
    load_tables()
    writer = pd.ExcelWriter('res.xlsx')
    for i, query in enumerate(querys):
        df = SELECT(query)
        df.to_excel(writer, sheet_name=str(i), index=False)
    writer.save()