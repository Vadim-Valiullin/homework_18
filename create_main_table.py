import psycopg2
from psycopg2 import Error
import csv


def connection():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="123456",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="cat_shelter_1")
        connection.autocommit = True
        print('Connection Done')
        return connection
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        return False


def create_table(cursor):
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cat_shelter (
             id SERIAL PRIMARY KEY,
             age_upon_outcome VARCHAR(30),
             animal_id VARCHAR(30),
             fk_animal_type INTEGER,
             animal_type VARCHAR(50),
             name VARCHAR(50),
             fk_breed INTEGER,
             breed VARCHAR(100),
             fk_color INTEGER,
             color VARCHAR(50),
             date_of_birth TIMESTAMP,
             fk_outcome_subtype INTEGER,
             outcome_subtype VARCHAR(30),
             fk_outcome_type INTEGER,
             outcome_type VARCHAR(50),
             outcome_month INTEGER,
             outcome_year INTEGER
        )
        """)
        print('Таблица создана')
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def get_data():
    list_data = []
    with open('main_animals.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        count = 0
        for row in reader:
            if count == 0:
                print(f'Файл содержит столбцы: {", ".join(row)}')
            else:
                s = row[1], row[2], row[3], row[4], row[5], row[6] + row[7], row[8], row[9], row[10], row[11], row[12]
                list_data.append(s)
            count += 1
        return list_data


def fill_table(cursor, list_data):
    for i in list_data:
        req = f"""
            INSERT INTO
                cat_shelter (age_upon_outcome, animal_id, animal_type, name, breed, color, date_of_birth, 
                outcome_subtype, outcome_type, outcome_month, outcome_year)
            VALUES
                {i}
            """
        try:
            cursor.execute(req)
            print('Данные внесены в таблицу')
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)



def main():
    conn = connection()
    if conn:
        cursor = conn.cursor()
        create_table(cursor)
        get_data()
        list_data = get_data()
        fill_table(cursor, list_data)

        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
