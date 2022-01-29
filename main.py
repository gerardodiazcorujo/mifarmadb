# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import mysql.connector as mysql
import re

def normalizeName(description):
    # Normalize the name of product to create master names
    try:
        new_description = description.upper()
        new_description = re.sub('(| |^)(\d{1,})(\.)(\d{1,})(| |$)', r' \2,\4 ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(l|ltr|ltr.|litros|litro)( |$)', r' \2l ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,}|\d{1,},\d{1,})( |)(ml|ml.)( |$)', r' \2ml ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(kilo|kilos|kg|kg.)( |$)', r' \2kg ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,}|\d{1,},\d{1,})( |)(gramos|gr|gr.|g|g.)( |$)', r' \2gr ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,}|\d{1,},\d{1,})( |)(miligramos|mg|mg.)( |$)', r' \2mg ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,}}|\d{1,},\d{1,})( |)(mg/ml)( |$)', r' \2mg/ml ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,}|\d{1,},\d{1,})( |)(cm|cm.)( |$)', r' \2cm ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,}|\d{1,},\d{1,})( |)(milimetro|ml|ml.)( |$)', r' \2ml ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(unidades|ud|uds|un|u|ud.|uds.|un.|u.)( |$)', r' \2ud ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(comprimidos|comp|comp.)( |$)', r' \2 Comprimidos ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(capsulas|cápsulas|caps|cáps|caps.|cáps.)( |$)', r' \2Caps ',new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(ampolla|ampollas|amp|ampo|amp.)( |$)', r' \2 Ampollas ',new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(\d{1,})( |)(INFUSIONES|BOLSITAS|BOLSAS|BOL|SOBRES|SOBRE|FILTROS|SBRS.)( |$)', r' \2 SBRS ',new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(loc|locion)( |$)', r' Loción ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(deso|desod|desodo)( |$)', r' Desodorante ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(spray|spr)( |$)', r' Spray ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(stick|stk)( |$)', r' Stick ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(efer|eferv)( |$)', r' Efervescentes ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('( |^)(efe)( |$)', r' Efecto ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('(SPF) (\d{1,})(|\+)( |$)', r'SPF\2 ', new_description, flags=re.IGNORECASE)
        new_description = re.sub('/[\s\S]*?(?= *$)/', r'', new_description, flags=re.IGNORECASE)
        new_description = re.sub(' +', ' ', new_description)
        new_description = re.sub('^[ \t]+|[ \t]+$', r'', new_description) #Remove spaces initial and end
    except AttributeError:
        new_description = ''
    return new_description

def change_new():
    try:
        connection = mysql.connect(
            host='vitalfar-db.c0qyljvblxsv.eu-west-1.rds.amazonaws.com',
            database='vitalfardb',
            user='vitalfardbadmin',
            password='hawpim-4coCna-modhyb')

        sql_select_Query = "select * from tmp_mifarma_new where newname is null and found = false "
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)
        for row in records:
            newname = normalizeName(row[2])
            #print('newname-> %s' % row[2])
            # print('newname-> %s'% newname)
            newcursor = connection.cursor()
            updatequery = 'Update tmp_mifarma_new set newname = %s where product_name = %s'
            # print('query-> %s' % updatequery)
            tuple1 = (newname, row[2])
            # print('value 1-> %s' % tuple1[0])
            # print('value 2-> %s' % tuple1[1])
            newcursor.execute(updatequery, tuple1)
            connection.commit()

    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
            print("MySQL connection is closed")

def change_old():
    try:
        connection = mysql.connect(
            host='vitalfar-db.c0qyljvblxsv.eu-west-1.rds.amazonaws.com',
            database='vitalfardb',
            user='vitalfardbadmin',
            password='hawpim-4coCna-modhyb')

        sql_select_Query = "select * from tmp_mifarma_old where newname is null and found = false "
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)
        for row in records:
            newname = normalizeName(row[2])
            #print('newname-> %s' % row[2])
            # print('newname-> %s'% newname)
            newcursor = connection.cursor()
            updatequery = 'Update tmp_mifarma_old set newname = %s where product_name = %s'
            # print('query-> %s' % updatequery)
            tuple1 = (newname, row[2])
            # print('value 1-> %s' % tuple1[0])
            # print('value 2-> %s' % tuple1[1])
            newcursor.execute(updatequery, tuple1)
            connection.commit()

    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
            print("MySQL connection is closed")

def change_old_modify_name():
    try:
        connection = mysql.connect(
            host='vitalfar-db.c0qyljvblxsv.eu-west-1.rds.amazonaws.com',
            database='vitalfardb',
            user='vitalfardbadmin',
            password='hawpim-4coCna-modhyb')

        sql_select_Query = "select * from tmp_mifarma_old where newname is null"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)
        for row in records:
            newname = normalizeName(row[2])
            brand = row[1].capitalize()
            newname = 'HEMAPRO BÁLSAMO 500 COSMETICS 60ml'
            brand = '500 Cosmetics'.upper()
            if newname.find(brand) != -1:
                newname = newname.replace(brand, '')
                newname = brand + ' '+ newname
                newname = " ".join(newname.split())
            print('newname-> %s' % row[2])
            # print('newname-> %s'% newname)
            newcursor = connection.cursor()
            updatequery = 'Update tmp_mifarma_old set newname = %s where product_name = %s'
            # print('query-> %s' % updatequery)
            tuple1 = (newname, row[2])
            # print('value 1-> %s' % tuple1[0])
            # print('value 2-> %s' % tuple1[1])
            newcursor.execute(updatequery, tuple1)
            connection.commit()

    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
            print("MySQL connection is closed")

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    change_new()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
