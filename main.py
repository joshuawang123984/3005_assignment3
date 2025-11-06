import psycopg
from configparser import ConfigParser

#opens the .sql file that creates the table and loads in the starting row values
def run_sql_file(filename):
    conn = get_connection()
    cur = conn.cursor()
    with open(filename, 'r') as f:
        sql = f.read()
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Executed {filename} successfully")

#configures the .ini file
def config(filename='database.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        return db

#connects to the database using the .ini file and returns the connection
def get_connection():
    params = config()
    return psycopg.connect(**params)

#selects all student row values from the table and orders by the student id, then returns the rows
def getAllStudents():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM students ORDER BY student_id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

#adds a student based on inputted arguments. returns the newly added students student id.
def addStudent(first_name, last_name, email, enrollment_date):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO students (first_name, last_name, email, enrollment_date)
        VALUES (%s, %s, %s, %s)
        RETURNING student_id;
        """,
        (first_name, last_name, email, enrollment_date)
    )
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return new_id

#uses a specified student id to update the students email in the students table returns true or false depending on success or failure.
def updateStudentEmail(student_id, new_email):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE students
        SET email = %s
        WHERE student_id = %s;
        """,
        (new_email, student_id)
    )
    updated = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return updated

#deletes specified student based on student id from the students table. returns true or false depending on success or failure
def deleteStudent(student_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM students
        WHERE student_id = %s;
        """,
        (student_id,)
    )
    updated = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return updated

#operation tests
def main():
    print("Current students")
    rows = getAllStudents()
    print(rows)

    print("\nAdding a new student")
    new_id = addStudent("Joshua", "Joshua", "joshua@example.com", "2023-09-03")
    print(f"Inserted student_id = {new_id}")

    print("\nStudents after insert")
    rows = getAllStudents()
    print(rows)

    print("\nUpdating Joshua's email")
    success = updateStudentEmail(new_id, "not_joshua@example.com")
    print("Update succeeded." if success else "Update failed (no such student).")

    print("\nStudents after update")
    rows = getAllStudents()
    print(rows)

    print("\nDeleting the new student")
    deleted = deleteStudent(new_id)
    print("Delete succeeded" if deleted else "Delete failed")

    print("\nStudents after delete")
    rows = getAllStudents()
    print(rows)

#first create the table and add values, then run operations  
if __name__ == '__main__':
    run_sql_file('students_table.sql')
    main()