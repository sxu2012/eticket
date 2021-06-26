import mysql.connector
from mysql.connector import errorcode

def create_database( cursor ):
	try:
		cursor.execute("CREATE DATABASE eticket DEFAULT CHARACTER SET 'utf8'")
	except mysql.connector.Error as err:
		print(err)
		exit(1)

def create_table( cursor ):
	table_sales = (
		"CREATE TABLE IF NOT EXISTS `sales` ("
		"  `ticket_id` INT(11) NOT NULL,"
		"  `trans_date` DATE,"
		"  `event_id` INT(11),"
		"  `event_name` VARCHAR(50),"
		"  `event_date` DATE,"
		"  `event_type` VARCHAR(10),"
		"  `event_city` VARCHAR(20),"
		"  `customer_id` INT(11),"
		"  `price` DECIMAL,"
		"  `num_tickets` INT(11),"
		"  PRIMARY KEY (`ticket_id`)"
		") ENGINE=InnoDB")
	try:
		cursor.execute(table_sales)
	except mysql.connector.Error as err:
		if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
			print("Table sales already exists")
		else:
			print(err.msg)
			exit(1)

def get_db_connection():
	cnx = mysql.connector.connect(user='root',
		password='1234',
		host='127.0.0.1',
		port='3306')
	cursor = cnx.cursor()
	try:
		cursor.execute("USE eticket")
	except mysql.connector.Error as err:
		print("Database eticket does not exists.")
		if err.errno == errorcode.ER_BAD_DB_ERROR:
			create_database(cursor)
			print("Database eticket created successfully")
			cursor.execute("USE eticket")
		else:
			print(err)
			exit(1)
	create_table(cursor)
	return cnx

def load_third_party(connection, file_path_csv):
	cursor = connection.cursor()
	add_row = ("INSERT INTO sales "
		   "(ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) "
		   "VALUES (%(ticket_id)s, %(trans_date)s, %(event_id)s, %(event_name)s, %(event_date)s, %(event_type)s, %(event_city)s, %(customer_id)s, %(price)s, %(num_tickets)s)")

	with open(file_path_csv,'r') as f:
		for line in f:
			la = line.strip().split(',')
			data_row = {
				'ticket_id': la[0],
				'trans_date': la[1],
				'event_id': la[2],
				'event_name': la[3],
				'event_date': la[4],
				'event_type': la[5],
				'event_city': la[6],
				'customer_id': la[7],
				'price': la[8],
				'num_tickets': la[9],
			}
			cursor.execute(add_row, data_row)
	connection.commit()
	cursor.close()
	return

def query_popular_tickets(connection):
	sql_statement = (
		"SELECT event_name, SUM(num_tickets) as total_tickets_sold "
		"FROM sales "
		"GROUP BY event_id "
		"ORDER BY total_tickets_sold DESC "
		"LIMIT 3"
	)
	cursor = connection.cursor()
	cursor.execute(sql_statement)
	print("Here are the most popular tickets: ")
	for (event_name, total_tickets_sold) in cursor:
		print("- "+event_name)
	cursor.close()
"""
 main entry point
"""
if __name__ == '__main__':
	cnx = get_db_connection()
	load_third_party(cnx, "third_party_sales_1.csv")
	query_popular_tickets(cnx)
	if cnx is not None:
		cnx.close()

"""
A sample execution of the program:

PS C:\study\sb\projects\eticket> python eticket.py
Database eticket does not exists.
Database eticket created successfully
Here are the most popular tickets: 
- Washington Spirits vs Sky Blue FC
- Christmas Spectacular
- The North American International Auto Show
"""