import pandas as pd
import math


def insert_all_data(connection,cursor):
    print("Started Inserting data")
    # restaurants=pd.read_csv('data/zomato-schema - restaurants.csv')
    # restaurants_cols=['id','r_name','cusine']
    # restaurants.columns=restaurants_cols
    # restaurants=pd.DataFrame(restaurants)
    restaurants=pd.DataFrame((pd.read_csv('data/restaurants.csv')))
    restaurants.columns=['id','r_name','cusine']


    # menu=pd.read_csv('data/menu.csv')
    # menu_cols=['id','r_id','f_id','price']
    # menu.columns=menu_cols
    # menu=pd.DataFrame(menu)

    menu=pd.DataFrame(pd.read_csv('data/menu.csv'))
    menu.columns=['id','r_id','f_id','price']


    # food=pd.read_csv('data/food.csv')
    # food_cols=['id','f_name','type']
    # food.columns=food_cols
    # food=pd.DataFrame(food)

    food=pd.DataFrame(pd.read_csv('data/food.csv'))
    food.columns=['id','f_name','type']

    # orders=pd.read_csv('data/orders.csv')
    # orders_cols=['id','user_id','r_id','amount','date','partner_id','delivery_time','delivery_rating','restaurant_rating']
    # orders.columns=orders_cols
    # orders=pd.DataFrame(orders)
    # order=orders.fillna('')

    orders=pd.DataFrame(pd.read_csv('data/orders.csv'))
    orders.columns=['id','user_id','r_id','amount','date','partner_id','delivery_time','delivery_rating','restaurant_rating']



    # order_details=pd.read_csv('data/order_details.csv')
    # orderdetails_cols=['id','order_id','f_id']
    # order_details.columns=orderdetails_cols
    # order_details=pd.DataFrame(order_details)

    order_details=pd.DataFrame(pd.read_csv('data/order_details.csv'))
    order_details.columns=['id','order_id','f_id']

    # partners=pd.read_csv('data/delivery_partner.csv')
    # partners_cols=['id','name']
    # partners.columns=partners_cols
    # partners=pd.DataFrame(partners)

    partners=pd.DataFrame(pd.read_csv('data/delivery_partner.csv'))
    partners.columns=['id','name']


    tables=[restaurants,food,menu,partners,orders,order_details]
    for i,table in enumerate(tables):

        if i == 0:
            table_name = "restaurants"
        elif i == 1:
            table_name = "food"
        elif i == 2:
            table_name = "menu"
        elif i == 3:
            table_name = "partners"
        elif i == 4:
            table_name = "orders"
        elif i == 5:
            table_name = "order_details"
        cols=table.columns
        for index,row in table.iterrows():
            try:
                id=row['id']
                query=f"select id from {table_name} where id ={id}"
                cursor.execute(query)
                existing_item=cursor.fetchone()
                if existing_item:
                    pass
                else:
                    col_data = ','.join([f"'{str(eachcol)}'" if isinstance(eachcol, str) and eachcol != '' else 'NULL' if (isinstance(eachcol, float) and math.isnan(eachcol)) else str(eachcol) for eachcol in row])
                    query=f"insert into {table_name} ({','.join(cols)}) values ({col_data});"
                    cursor.execute(query)
                    connection.commit()
            except:
                print('Error Inserting Data')
                return 'Error'

    print("Inserted data Into DataBase Successfully")
    return True
