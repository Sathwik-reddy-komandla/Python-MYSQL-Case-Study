import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os
from dataReader import insert_all_data
from  userDataReader import insert_users

load_dotenv()

host ="localhost"
user="root"
password=os.getenv("password")
database=os.getenv('DataBase')


def create_conection():
    try:
        connection=mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            
    except mysql.connector.Error as e:
        print("Error:", e)


    cursor=connection.cursor()
    return connection,cursor


def create_tables(connection,cursor):
    # SQL Queries for Creating Tables 

    create_user_table_query="""create table Users (
                id int auto_increment primary key,
                name varchar(255) not null,
                email varchar(255) not null,
                password varchar(255) not null
    ) """

    create_restaurant_table_query="""create table Restaurants (
                    id int Auto_Increment Primary Key,
                r_name varchar(255) not null,
                cusine varchar(255) not null
                )"""

    create_food_table_query="""create table Food (
    id int Auto_Increment Primary Key,
                f_name varchar(255) not null,
                type varchar(255) not null
                )"""

    create_menu_table_query="""create table Menu (
    id int Auto_Increment Primary Key,
                r_id int not null,
                f_id int not null,
                price int not null,
                foreign key (r_id) references Restaurants(id),
                foreign key (f_id) references food(id)

                                )"""

    create_partner_table_query="""create table Partners (
                id int Auto_Increment Primary Key,
                name varchar(255) not null
                    )"""

    create_orderdetails_table_query="""create table Order_details (
                id int Auto_Increment Primary Key,
                order_id int not null,
                f_id int not null,
                foreign key (f_id) references Food(id)
                )"""

    create_orders_table_query="""create table Orders (
                id int Auto_Increment Primary Key,
                user_id int not null,
                r_id int not null,
                amount int not null,
                date Date not null,
                partner_id int not null,
                delivery_time int not null,
                delivery_rating int not null,
                restaurant_rating int,
                foreign key (r_id) references Restaurants(id),
                foreign key (user_id) references Users(id),
                foreign key (partner_id) references Partners(id)

                                )"""


    # To Check if a Table is Already present
    def check_table(database,table):
        check_table_query=f"select 1 from Information_schema.tables where table_schema = '{database}' and table_name = '{table}' Limit 1"

        cursor.execute(check_table_query)
        results = cursor.fetchone()
        return results


    # check if users table exists
    table='Users'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_user_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")
        except :
            print('Error Occured')


    # check if Restaurant table exists
    table='Restaurants'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_restaurant_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")
        except:
            print('Error Occured. ')    


    # check if Food table exists
    table='Food'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_food_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")
        except:
            print('Error Occured.')

    # check if Menu table exists
    table='Menu'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_menu_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")
        except:
            print('Error Occured. ')

        
    # check if partner table exists
    table='Partners'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_partner_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")
        except:
            print('Error Occured. ')

    # check if orders table exists
    table='Orders'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_orders_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")
        except:
            print('Error Occured. ')

    # check if orderdetails table exists
    table='Order_details'
    result=check_table(database,table)
    if result is not None:
        pass
    else:
        try:
            cursor.execute(create_orderdetails_table_query)
            creation = cursor.fetchall()
            if creation is not None:
                print(f"Table  {table} Created SuccessFully")
            else:
                print('******Error*******')
                print(f"Error Creating Table  {table} ")

        except:
            print('Error Occured')


def main():
    print(__name__)
    connection,cursor=create_conection()
    create_tables(connection,cursor)
    users=insert_users(connection=connection,cursor=cursor)
    if users:
        inserted=insert_all_data(connection=connection,cursor=cursor)
        if inserted:
            print("Insertion Completed Successfully")
        else:
            print("Error Inserting data")

    cursor.close()
    connection.close()

  
if __name__=='__main__':
    main()





