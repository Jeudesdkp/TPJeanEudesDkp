# bibliothéques necessaires pour cette procédure, 
# pandas pour l'importation de la donnée qui est en format CSV
# et mysql-connector-python pour la connexion MySql

#pip install mysql-connector-python (pour son installation)

#Importation des modules necessaires
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Chargement de la donnée
donneeGSTC = pd.read_csv("C:\training\CC.csv")

print(donneeGSTC.columns) # assurons nous que la base sql vide qui va recevoir les données a exactement les mêmes noms de colonnes

#on peut renommer à notre guise
colonnes = ['country',	'is03',	'indicator',	'unit',	'source',	'1961',
            	'1962',	'1963',	'1964',	'1965',	'1966',	'1967',	'1968',	'1969',
                    	'1970',	'1971'	'1972',	'1973',	'1974',	'1975',	'1976',	'1977',	'1978',
                            	'1979',	'1980',	'1981',	'1982',	'1983',	'1984',	'1985',	'1986',	'1987',	'1988',
                                    	'1989',	'1990',	'1991',	'1992',	'1993',	'1994',	'1995',	'1996',	'1997',	'1998',	'1999',
                                            	'2000',	'2001',	'2002',	'2003',	'2004',	'2005',	'2006',	'2007',	'2008',	'2009',
                                                    	'2010',	'2011',	'2012',	'2013',	'2014',	'2015',	'2016',	'2017',	'2018',
                                                            	'2019',	'2020',	'2021',	'2022',	'subregion',	'incomegroup',
                                                                    	'area', 'density']
donneeGSTC.columns = colonnes 
print('----------------------------------------------------------------')
print(donneeGSTC.columns)

# pour afficher les statistiques descriptives de notre données 
print("Summary Statistics:")
print(donneeGSTC.describe())

# vérifions si notre donnee a de valeurs manquantes (pas de données manquantes dans notre base)
donneeGSTC.isnull().sum()

# vérifions les types des colonnes de notre donnée ('temperature change' n'est pas dans le bon format, à convertir en numeric)
print("\nData Types:")
print(df.dtypes)

# chargeons à présent notre base de donnée dans MySql

# creer la base de donnees 
# use baseTP

try:
    connexiondb = mysql.connector.connect(host='localhost',
                                       database='baseTP',
                                       user='root',
                                       password='')
    if connexiondb.is_connected():
        print('Connexion à MySQL réussie')
except Error as e:
    print(f"Erreur lors de la connexion à MySQL: {e}")

try:
    cursor = connexiondb.cursor()

    for i,row in donneeGSTC.iterrows():
        sql = """INSERT INTO temps_data (country,isO3,indicator,unit,source,1961,1962,1963,1964,1965,1966,1967,
        1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,
            	1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,
                    	2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,subregion,
                            	incomegroup,area,density) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" 
        # temps_data est la table créée dans mysql
        
        cursor.execute(sql, tuple(row))

    connexion.commit()
    connexion.close()
    print("DataFrame chargé dans MySQL avec succès!")
except Exception as e:
    print(f"Erreur lors du chargement du fichier CSV dans MySQL: {e}")


## pour pusher le code 
    
# git https://github.com/Jeudesdkp/TPJeanEudesDkp.git   (pour cloner le repository de github)   
# git add . (pour ajouter tous les fichiers)  
# git config --global user.email "jeudesd@gmail.com"      
# git config --global user.name "Jeudesdkp"
# git commit -m "Initialisation de projet"
# git push origin main (pour envoyer les fichiers)             
