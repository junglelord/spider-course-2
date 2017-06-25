import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
import httplib
import hashlib
import time 

class CrawlDatabaseManager:

    DB_NAME = 'mfw_pro_crawl'

    SERVER_IP = '127.0.0.1'

    TABLES = {}
    TABLES['urls'] = (
        "CREATE TABLE `urls` ("
        "  `index` int(11) NOT NULL AUTO_INCREMENT,"
        "  `url` varchar(512) NOT NULL,"
        "  `md5` varchar(32) NOT NULL,"
        "  `status` varchar(11) NOT NULL DEFAULT 'new',"
        "  `depth` int(11) NOT NULL,"
        "  `queue_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "  `done_time` timestamp NOT NULL DEFAULT 0,"
        "  PRIMARY KEY (`index`),"
        "  UNIQUE KEY `md5` (`md5`)"
        ") ENGINE=InnoDB")


    def __init__(self, max_num_thread):
        try:
            cnx = mysql.connector.connect(host=self.SERVER_IP, user='root')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print 'Create Error ' + err.msg
            exit(1)

        cursor = cnx.cursor()

        try:
            cnx.database = self.DB_NAME  
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database(cursor)
                cnx.database = self.DB_NAME
                self.create_tables(cursor)
            else:
                print(err)
                exit(1)
        finally:
            cursor.close()
            cnx.close()

        dbconfig = {
            "database": self.DB_NAME,
            "user":     "root",
            "host":     self.SERVER_IP,
        }
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "mypool",
                                                          pool_size = max_num_thread,
                                                          **dbconfig)


    def create_database(self, cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_tables(self, cursor):
        for name, ddl in self.TABLES.iteritems():
            try:
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print 'create tables error ALREADY EXISTS'
                else:
                    print 'create tables error ' + err.msg
            else:
                print 'Tables created'


    def enqueueUrl(self, url, depth):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            add_url = ("INSERT INTO urls (url, md5, depth) VALUES (%s, %s, %s)")
            data_url = (url, hashlib.md5(url).hexdigest(), depth)
            cursor.execute(add_url, data_url)
            con.commit()
        except mysql.connector.Error as err:
            # print 'enqueueUrl() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()


    def dequeueUrl(self):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT `index`, `url`, `depth` FROM urls WHERE status='new' ORDER BY `index` ASC LIMIT 1 FOR UPDATE")
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None
            row = cursor.fetchone()
            update_query = ("UPDATE urls SET `status`='downloading' WHERE `index`=%d") % (row['index'])
            cursor.execute(update_query)
            con.commit()
            return row
        except mysql.connector.Error as err:
            # print 'dequeueUrl() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()

    def finishUrl(self, index):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            # we don't need to update done_time using time.strftime('%Y-%m-%d %H:%M:%S') as it's auto updated
            update_query = ("UPDATE urls SET `status`='done', `done_time`=%s WHERE `index`=%d") % (time.strftime('%Y-%m-%d %H:%M:%S'), index)
            cursor.execute(update_query)
            con.commit()
        except mysql.connector.Error as err:
            # print 'finishUrl() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()
