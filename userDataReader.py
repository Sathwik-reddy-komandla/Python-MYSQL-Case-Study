import pandas as pd

def insert_users(connection,cursor):
    print("Started Inserting User data")
    users=pd.read_csv('data/users.csv')

    columns=['id','name','email','password']

    df=pd.DataFrame(users)

    existing_ids=set()

    for index,row in users.iterrows():
        user_id=row['user_id']
        query=f'Select id from Users where id={user_id}'
        cursor.execute(query)
        existing_user=cursor.fetchone()
        if existing_user:
            existing_ids.add(user_id)




    data_to_insert=[[row['user_id'],row['name'],row['email'],row['password']] for index, row in users.iterrows() if row['user_id'] not in existing_ids]

    if data_to_insert:
        query="Insert into users (id,name,email,password) Values (%s, %s, %s ,%s)"
        cursor.executemany(query,data_to_insert)
        connection.commit()
        print(f"{len(data_to_insert)} rows inserted into table")

    print('Users Inserted Successfully')
    return True