from create_main_table import *
from psycopg2 import Error


def create_new_tables(cursor):
    try:
        cursor.execute("""
            SELECT DISTINCT breed
            INTO breed_table
            FROM cat_shelter;
            
            ALTER TABLE breed_table
            ADD COLUMN id_breed SERIAL PRIMARY KEY;
            
            SELECT DISTINCT color
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
            
            SELECT DISTINCT id, animal_id, animal_type, name, breed_table.id_breed, color_table.id_color, date_of_birth
            INTO cats_table
            FROM cat_shelter
            JOIN breed_table ON breed_table.breed = cat_shelter.breed
            JOIN color_table ON color_table.color = cat_shelter.color
            GROUP BY id, animal_id, animal_type, name, breed_table.id_breed, color_table.id_color, date_of_birth;
            
            ALTER TABLE IF EXISTS cats_table
            ADD PRIMARY KEY (id);
            
            SELECT cat_shelter.id, age_upon_outcome, cats_table.animal_id, outcome_subtype_table.id_outcome_subtype, 
            outcome_type_table.id_outcome_type, outcome_month, outcome_year
            INTO shelter
            FROM cat_shelter
            JOIN cats_table ON cats_table.animal_id = cat_shelter.animal_id
            JOIN outcome_subtype_table ON outcome_subtype_table.outcome_subtype = cat_shelter.outcome_subtype
            JOIN outcome_type_table ON outcome_type_table.outcome_type = cat_shelter.outcome_type
            GROUP BY cat_shelter.id, age_upon_outcome, cats_table.animal_id, outcome_subtype_table.id_outcome_subtype, 
            outcome_type_table.id_outcome_type, outcome_month, outcome_year;
            
            ALTER TABLE IF EXISTS shelter
            ADD PRIMARY KEY (id);
            
            ALTER TABLE cats_table
            ADD CONSTRAINT fk_cat_breed FOREIGN KEY (id_breed) REFERENCES breed_table (id_breed);
            
            ALTER TABLE cats_table
            ADD CONSTRAINT fk_cat_color FOREIGN KEY (id_color) REFERENCES color_table (id_color);
            
            ALTER TABLE shelter
            ADD CONSTRAINT fk_cat_subtype FOREIGN KEY (id_outcome_subtype) REFERENCES outcome_subtype_table (id_outcome_subtype);
            
            ALTER TABLE shelter
            ADD CONSTRAINT fk_cat_outcome_type FOREIGN KEY (id_outcome_type) REFERENCES outcome_type_table (
            id_outcome_type);
            
            ALTER TABLE shelter
            ADD CONSTRAINT fk_cat_shelter FOREIGN KEY (id) REFERENCES cats_table (id);
            
            CREATE USER Bonnie;

            GRANT SELECT ON shelter TO Bonnie;
            
            SELECT * FROM information_schema.table_privileges WHERE grantee='Bonnie';
            
            ALTER USER Bonnie WITH PASSWORD '1234';
            
            CREATE USER Clide;

            GRANT SELECT, INSERT, UPDATE ON shelter TO Clide;
            
            SELECT * FROM information_schema.table_privileges WHERE grantee='Clide';
            
            ALTER USER Clide WITH PASSWORD '1234'
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
