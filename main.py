from datetime import datetime
from datetime import date
import mysql.connector
from tabulate import tabulate
from dateutil.relativedelta import relativedelta
import re
import sys


try:
    mydb = mysql.connector.connect(host="localhost", user="root", passwd="", database="inventory_management_db")
    cursor = mydb.cursor()
    active_customer = None
    total = 0
    bill_list = []

except mysql.connector.errors.ProgrammingError:
    print("Please create a database named 'inventory_management_db' ")
    sys.exit()

#creating tables
sql="create table if not exists customers(customerName varchar(30) not null,mobileNumber varchar(20), joinDate timestamp, primary key(customerName,mobileNumber));"
cursor.execute(sql)
mydb.commit()

sql="create table if not exists products(productName varchar(30) not null unique,productKey varchar(10) unique, productCount int, productCost double, productLastModifiedDate date, primary key(productName,productKey));"
cursor.execute(sql)
mydb.commit()

sql="create table if not exists transactions(transactionDate timestamp,customerName varchar(30), productName varchar(30),productCount int, productCostperUnit double,productAmount double);"
cursor.execute(sql)
mydb.commit()

sql="create table if not exists productEntries(productName varchar(30) not null ,productKey varchar(10) , productCount int, productCost double, productLastModifiedDate date);"
cursor.execute(sql)
mydb.commit()

def add_customer(customer_name, mobile_number, current_date):
    try:
        sql = "INSERT INTO customers values(%s,%s,%s) "
        values = (customer_name, mobile_number, current_date)
        cursor.execute(sql, values)
        mydb.commit()
        print("Customer Account created")
    except mysql.connector.errors.IntegrityError:
        print("Customer account already created ")


def write_product(pro_key, pro_count, current_date):
    if active_customer is None:
        print("ERROR : Please Create customer account")
    else:
        sql = "SELECT * FROM products WHERE productKey=%s"
        values = [pro_key]
        cursor.execute(sql, values)
        results = cursor.fetchall()
        try:
            product_count = results[0][2]
            if pro_count > 0:
                if pro_count < product_count:
                    sql1 = "INSERT INTO transactions VALUES(%s,%s,%s,%s,%s,%s)"
                    values1 = [current_date, active_customer, results[0][0], pro_count, results[0][3],
                               (results[0][3] * pro_count)]
                    cursor.execute(sql1, values1)
                    mydb.commit()

                    sql2 = "UPDATE products SET productCount=%s WHERE productName=%s"
                    change_product_count = results[0][2] - pro_count
                    values2 = [change_product_count, results[0][0]]
                    cursor.execute(sql2, values2)
                    mydb.commit()

                    global total
                    total = total + (pro_count * results[0][3])
                    bill_statement = "   %10s  ->  %3d    *   %5.1f  =  %7.1f" % (
                        (str(results[0][0]), int(pro_count), float(results[0][3]), float(results[0][3] * pro_count)))
                    bill_list.append(bill_statement)
                    print(bill_statement)
                else:
                    print(" ERROR: You do not have sufficient product ")
            else:
                print("ERROR: Invalid product count")

        except:
            print(" ERROR : Invalid Product Key")
            

def add_product(pro_name, pro_key, pro_count, pro_cost, current_date):
    try:
        sql = "INSERT INTO products values(%s,%s,%s,%s,%s)"
        values = (pro_name, pro_key, pro_count, pro_cost, current_date)
        cursor.execute(sql, values)
        mydb.commit()

        sql = "INSERT INTO productEntries values(%s,%s,%s,%s,%s)"
        values = (pro_name, pro_key, pro_count, pro_cost, current_date)
        cursor.execute(sql, values)
        mydb.commit()
        print("Product added successfully")
    except mysql.connector.errors.IntegrityError:
        print(" ERROR: This product name is already added")


def update_product(pro_name, pro_key, pro_count, pro_cost, current_date):
    sql = "SELECT * FROM products WHERE (productName= %s)"
    values = [pro_name]
    cursor.execute(sql, values)
    results = cursor.fetchall()
    if len(results) == 0:
        print(" ERROR: Product name not found. please check product details ")
    else:

        sql = "UPDATE products SET productKey=%s,productCount=%s,productCost=%s,productLastModifiedDate=%s WHERE " \
              "productName=%s "
        if re.match("\+|-.", pro_count):
            pro_count = int(pro_count)
            values = [pro_key, (results[0][2] + pro_count), pro_cost, current_date, pro_name]
        else:
            pro_count = int(pro_count)
            values = [pro_key, pro_count, pro_cost, current_date, pro_name]
        cursor.execute(sql, values)
        mydb.commit()
        sql1 = "INSERT INTO productEntries values(%s,%s,%s,%s,%s) "
        values1 = [pro_name, pro_key, pro_count, pro_cost, current_date]
        cursor.execute(sql1, values1)
        mydb.commit()
        print("Product Updated Successfully")


def remove_product(pro_name, current_date):
    sql = "SELECT * FROM products WHERE productName=%s"
    values = [pro_name]
    cursor.execute(sql, values)
    results = cursor.fetchall()
    if len(results) == 0:
        print(" ERROR: Product name not found. please check product details")
    else:
        sql = "INSERT INTO productEntries values(%s,%s,%s,%s,%s)"
        values = (pro_name, '--', '--', '--', current_date)
        cursor.execute(sql, values)
        mydb.commit()

        sql = "DELETE FROM products WHERE productName=%s"
        values = [pro_name]
        cursor.execute(sql, values)
        mydb.commit()
        print("Product removed successfully")


def product_list():
    sql = "SELECT * FROM products "
    cursor.execute(sql)
    results = cursor.fetchall()
    print(tabulate(results, ["Product Name ", "Key", "Count", "Cost", "Last Modified Date"], tablefmt="grid"))


def history(given_date, sql):
    today = date.today()
    tomorrow = today + (relativedelta(days=1))
    yesterday = today - (relativedelta(days=1))
    last_week = today - (relativedelta(days=7))
    last_month = today - (relativedelta(months=1))

    if given_date == 'today':
        values = [today, tomorrow]
        cursor.execute(sql, values)
        results = cursor.fetchall()
        mydb.commit()
        return results
    elif given_date == 'yesterday':
        values = [yesterday, today]
        cursor.execute(sql, values)
        results = cursor.fetchall()
        return results
    elif given_date == 'lastweek':
        values = [last_week, tomorrow]
        cursor.execute(sql, values)
        results = cursor.fetchall()
        return results
    elif given_date == 'lastmonth':
        values = [last_month, tomorrow]
        cursor.execute(sql, values)
        results = cursor.fetchall()
        return results
    elif given_date == "all":
        sql = "SELECT * FROM transactions"
        cursor.execute(sql)
        results = cursor.fetchall()
        return results
    elif given_date == "custom":
        from_date = input("Enter From Date : 'yyyy-mm-dd' ")
        to_date = input("Enter To Date : 'yyyy-mm-dd' ")
        values = [from_date, to_date]
        cursor.execute(sql, values)
        results = cursor.fetchall()
        return results

    else:
        print(" ERROR: Invalid Date format")
        return None


def particular_search(name, sql):
    values = [name]
    cursor.execute(sql, values)
    results = cursor.fetchall()
    return results


def customer_list():
    sql = "SELECT * FROM customers"
    cursor.execute(sql)
    results = cursor.fetchall()
    print(tabulate(results, ["Customer Name", "Mobile Number", "Joining Date"], tablefmt="grid"))


def main_method():
    current_date = datetime.now()
    print("Create a new customer using the command 'create [name] [mobile number]' ")
    while True:
        query = str(input())

        if query.upper() == "QUIT":
            break
        input_data = query.split()

        if input_data[0].upper() == 'CREATE':
            if len(input_data) == 3:
                customer_name = input_data[1]
                mobile_number = input_data[2]
                add_customer(customer_name, mobile_number, current_date)
                global active_customer, total, bill_list
                active_customer = customer_name
                total = 0
                bill_list = []
            else:
                print(" COMMAND ERROR : Please use this command  'create [name] [mobile number]' ")

        elif input_data[0].upper() == 'WRITE':
            if len(input_data) == 3:
                pro_key = input_data[1]
                pro_count = int(input_data[2])
                if active_customer is None:
                    print(" ERROR : Please create customer account")
                else:
                    write_product(pro_key, pro_count, current_date)
            else:
                print(" COMMAND ERROR : Please use this command  'write [product key] [count]' ")

        elif input_data[0].upper() == 'ADD':
            if len(input_data) == 5:
                pro_name = input_data[1]
                pro_key = input_data[2]
                pro_count = int(input_data[3])
                pro_cost = float(input_data[4])
                add_product(pro_name, pro_key, pro_count, pro_cost, current_date)
            else:
                print(" COMMAND ERROR : Please use this command  'add [product name] [product key] [count] [cost]' ")

        elif input_data[0].upper() == 'UPDATE':
            if len(input_data) == 5:
                pro_name = input_data[1]
                pro_key = input_data[2]
                pro_count = (input_data[3])
                pro_cost = float(input_data[4])
                update_product(pro_name, pro_key, pro_count, pro_cost, current_date)
            else:
                print("COMMAND ERROR : Please use this command  'update [product name] [product key] { +count / "
                      "-count / count } [cost]' ")

        elif input_data[0].upper() == 'REMOVE':
            if len(input_data) == 2:
                pro_name = input_data[1]
                remove_product(pro_name, current_date)
            else:
                print(" COMMAND ERROR : Please use this command  'remove [product name] ' ")

        elif input_data[0].upper() == 'PRODUCT':
            if len(input_data) == 2:
                pro_name = input_data[1]
                sql = "SELECT * FROM productEntries WHERE productName=%s"
                results = particular_search(pro_name, sql)
                if results is None:
                    print(" ERROR: Product name not found. Check product list")
                else:
                    print(
                        tabulate(results, ["Product Name", "Key", "Count", "Cost", "Last modified"], tablefmt="grid"))

            elif len(input_data) == 3:
                if input_data[1].upper() == 'HISTORY':
                    given_date = input_data[2]
                    if given_date!='all':
                        sql = "SELECT * FROM productEntries WHERE (productLastModifiedDate>=%s AND " \
                              "productLastModifiedDate<%s) "
                        results = history(given_date,sql)
                    else:
                        
                        sql = "SELECT * FROM productEntries"
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        
                    if results is None:
                        print("---- NIL ----")
                    else:
                        print(
                            tabulate(results,
                                     ["Product Name", "Key", "Count", "Cost", "Last Modified"],
                                     tablefmt="grid"))

            else:
                print(
                    "COMMAND ERROR : Please use this command  'product [product name] ' or 'product history {today, "
                    "yesterday,lastweek,lastmonth,all,custom}' ")

        elif input_data[0].upper() == 'PRODUCTS':
            if len(input_data) == 1:
                product_list()
            else:
                print(" COMMAND ERROR : Please use this command  ' products ' ")

        elif input_data[0].upper() == 'HISTORY':
            if len(input_data) == 2:
                given_date = input_data[1]
                sql = "SELECT * FROM transactions WHERE (transactionDate>=%s AND transactionDate<=%s);"
                results = history(given_date, sql)
                if results is None:
                    print("---- NIL ----")
                else:
                    print(
                        tabulate(results,
                                 ["Transaction Date", "Customer Name", "Product Name", "Count", "Cost", "Amount"],
                                 tablefmt="grid"))

            else:
                print("COMMAND ERROR : Please use this command  ' history {today, yesterday, lastweek, lastmonth, "
                      "custom, all} ' ")

        elif input_data[0].upper() == 'BILL':
            if len(input_data) == 1:
                for i in bill_list:
                    print(i)
                print(" %34s  =  %7.1f" % ('TOTAL', total))
            else:
                print(" COMMAND ERROR : Please use this command  ' bill ' ")

        elif input_data[0].upper() == 'CUSTOMER':
            if len(input_data) == 2:
                customer_name = input_data[1]
                sql = "SELECT * FROM transactions WHERE customerName=%s"
                results = particular_search(customer_name, sql)
                if results is None:
                    print(" ERROR: Customer name not found. Check customer list")
                else:
                    print(
                        tabulate(results, ["Date", "Customer Name", "Product Name", "Count", "Cost", "Amount"], tablefmt="grid"))
            else:
                print(" COMMAND ERROR : Please use this command  'customer [customer name] ' ")

        elif input_data[0].upper() == 'CUSTOMERS':
            if len(input_data) == 1:
                customer_list()
            else:
                print(" COMMAND ERROR : Please use this command  'customers ' ")
        else:
            print("INVALID COMMAND : Commands are - create, write, add, update, remove, bill, customer, customers, "
                  "product, products, history ")


if __name__ == '__main__':
    main_method()
