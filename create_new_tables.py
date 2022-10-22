import psycopg2
from psycopg2 import Error


def connection():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="123456",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="cat_shelter")
        connection.autocommit = True
        print('Connection Done')
        return connection
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        return False


def create_new_tables(cursor):
    try:
        cursor.execute("""
            SELECT DISTINCT animal_type
            INTO animal_type_table
            FROM cat_shelter;
            
            ALTER TABLE animal_type_table
            ADD COLUMN id_animal_type SERIAL PRIMARY KEY;
            
            SELECT DISTINCT breed
            INTO breed_table
            FROM cat_shelter;
            
            ALTER TABLE breed_table
            ADD COLUMN id_breed SERIAL PRIMARY KEY;
            
            SELECT DISTINCT color1, color2
            INTO color_table
            FROM cat_shelter;
            
            ALTER TABLE color_table
            ADD COLUMN id_color SERIAL PRIMARY KEY;
            
            SELECT DISTINCT outcome_subtype
            INTO outcome_subtype_table
            FROM cat_shelter;
            
            ALTER TABLE outcome_subtype_table
            ADD COLUMN id_outcome_subtype SERIAL PRIMARY KEY;
            
            SELECT DISTINCT outcome_type
            INTO outcome_type_table
            FROM cat_shelter;
            
            ALTER TABLE outcome_type_table
            ADD COLUMN id_outcome_type SERIAL PRIMARY KEY;
            
            SELECT age_upon_outcome, animal_id, animal_type_table.id_animal_type, name, breed_table.id_breed, color_table.id_color, date_of_birth, outcome_subtype_table.id_outcome_subtype, outcome_type_table.id_outcome_type
            INTO new_shelter
            FROM cat_shelter
            JOIN animal_type_table ON animal_type_table.animal_type = cat_shelter.animal_type
            JOIN breed_table ON breed_table.breed = cat_shelter.breed
            JOIN color_table ON color_table.color = cat_shelter.color
            JOIN outcome_subtype_table ON outcome_subtype_table.outcome_subtype = cat_shelter.outcome_subtype
            JOIN outcome_type_table ON outcome_type_table.outcome_type = cat_shelter.outcome_type
            GROUP BY age_upon_outcome, animal_id, animal_type_table.id_animal_type, name, breed_table.id_breed, color_table.id_color, date_of_birth, outcome_subtype_table.id_outcome_subtype, outcome_type_table.id_outcome_type;
            
            ALTER TABLE new_shelter
            ADD COLUMN id_cat_shelter SERIAL PRIMARY KEY;
            
            ALTER TABLE new_shelter
            ADD CONSTRAINT fk_cat_type FOREIGN KEY (id_animal_type) REFERENCES animal_type_table (id_animal_type);
            
            ALTER TABLE new_shelter
            ADD CONSTRAINT fk_cat_breed FOREIGN KEY (id_breed) REFERENCES breed_table (id_breed);
            
            ALTER TABLE new_shelter
            ADD CONSTRAINT fk_cat_color FOREIGN KEY (id_color) REFERENCES color_table (id_color);
            
            ALTER TABLE new_shelter
            ADD CONSTRAINT fk_cat_subtype FOREIGN KEY (id_outcome_subtype) REFERENCES outcome_subtype_table (id_outcome_subtype);
           
            ALTER TABLE new_shelter
            ADD CONSTRAINT fk_cat_outcome_type FOREIGN KEY (id_outcome_type) REFERENCES outcome_type_table (
            id_outcome_type);
            
            CREATE USER test_1;

            GRANT SELECT ON cat_shelter TO test_1;
            
            SELECT * FROM information_schema.table_privileges WHERE grantee='test_1';
            
            ALTER USER test_1 WITH PASSWORD '1234';
            
            CREATE USER Robin;

            GRANT SELECT, INSERT, UPDATE ON cat_shelter TO Robin;
            
            SELECT * FROM information_schema.table_privileges WHERE grantee='Robin';
            
            ALTER USER Robin WITH PASSWORD '1234'
        """)
        print('Таблица создана')
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def main():
    conn = connection()
    if conn:
        cursor = conn.cursor()
        create_new_tables(cursor)

        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
