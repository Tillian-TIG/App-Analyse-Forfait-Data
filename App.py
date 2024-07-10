from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Charger les données depuis les fichiers Excel
data = pd.read_excel("D:\\Programe\\Data\\")
mixte_voix = pd.read_excel("D:\\Programe\\Data\\")
pending = pd.read_excel("D:\\Programe\\Data\\")

# Fusionner les fichiers 'data' et 'mixte_voix' en une seule DataFrame
file_succes = pd.concat([data, mixte_voix])

# Sélectionner les colonnes nécessaires
file_succes = file_succes[['FRMSISDN', 'AMOUNT', 'TIMESTAMP']]

# Filtrer les transactions en attente ('PENDING') liées à l'airtime
options = ['AIRD', 'APPAIRD']
airtime_pending = pending[pending['TYPE'].isin(options)]
airtime_pending = airtime_pending[['REFERENCEID', 'TYPE', 'FRMSISDN', 'AMOUNT', 'TIMESTAMP']]

# Fonction pour convertir et formater la date au format souhaité
def convert_format_date_pending(dte): 
    date_obj = datetime.strptime(dte, '%Y-%m-%d %H:%M:%S.%f')  # Convertir la chaîne de caractères en objet datetime
    formatted_date = date_obj.strftime('%d/%m/%Y %H:%M:%S')  # Formater la date
    return formatted_date

# Appliquer la fonction de conversion et de formatage à la colonne TIMESTAMP de airtime_pending
airtime_pending['TIMESTAMP'] = airtime_pending['TIMESTAMP'].apply(convert_format_date_pending)

# Fonction pour convertir et formater la date pour file_succes
def convert_format_date_succes(dte):
    date_obj = datetime.strptime(str(dte), '%d/%m/%Y %H:%M:%S')  # Convertir la chaîne de caractères en objet datetime
    return date_obj

# Convertir le TIMESTAMP de file_succes en objet datetime
file_succes['TIMESTAMP'] = file_succes['TIMESTAMP'].apply(convert_format_date_succes)

# Définir la marge de temps (60 secondes)
time_margin = timedelta(seconds=60)

# Fonction pour déterminer l'action (SUCCES ou ROLLBACK) avec une marge de 60 secondes
def determine_action(row, succes_data):
    pending_time = datetime.strptime(row['TIMESTAMP'], '%d/%m/%Y %H:%M:%S')  # Convertir le timestamp de la ligne en datetime
    time_range_start = pending_time - time_margin  # Plage de temps début
    time_range_end = pending_time + time_margin  # Plage de temps fin
    # Filtrer les lignes de succes_data pour lesquelles TIMESTAMP est dans la plage de temps
    matching_success = succes_data[
        (succes_data['TIMESTAMP'] >= time_range_start) &
        (succes_data['TIMESTAMP'] <= time_range_end) &
        (succes_data['FRMSISDN'] == row['FRMSISDN']) &
        (succes_data['AMOUNT'] == row['AMOUNT'])
    ]
    if not matching_success.empty:
        return 'SUCCES'
    else:
        return 'ROLLBACK'

# Fonction pour retourner le timestamp de succès
def timestamp_succes(row, succes_data):
    pending_time = datetime.strptime(row['TIMESTAMP'], '%d/%m/%Y %H:%M:%S')
    time_range_start = pending_time - time_margin
    time_range_end = pending_time + time_margin
    matching_success = succes_data[
        (succes_data['TIMESTAMP'] >= time_range_start) &
        (succes_data['TIMESTAMP'] <= time_range_end) &
        (succes_data['FRMSISDN'] == row['FRMSISDN']) &
        (succes_data['AMOUNT'] == row['AMOUNT'])
    ]
    if not matching_success.empty:
        matching_time = matching_success['TIMESTAMP'].values[0]
        matching_time = pd.to_datetime(matching_time).to_pydatetime()
        return matching_time.strftime('%d/%m/%Y %H:%M:%S')  # Retourne la date formatée
    else:
        return 'No Match'

# Fonction pour retourner le montant de succès
def amount_succes(row, succes_data):
    pending_time = datetime.strptime(row['TIMESTAMP'], '%d/%m/%Y %H:%M:%S')
    time_range_start = pending_time - time_margin
    time_range_end = pending_time + time_margin
    matching_success = succes_data[
        (succes_data['TIMESTAMP'] >= time_range_start) &
        (succes_data['TIMESTAMP'] <= time_range_end) &
        (succes_data['FRMSISDN'] == row['FRMSISDN']) &
        (succes_data['AMOUNT'] == row['AMOUNT'])
    ]
    if not matching_success.empty:
        return int(matching_success['AMOUNT'].values[0])
    else:
        return 'No Match'
    
# Fonction pour retourner le FRMSISDN de succès
def frmsisdn_succes(row, succes_data):
    pending_time = datetime.strptime(row['TIMESTAMP'], '%d/%m/%Y %H:%M:%S')
    time_range_start = pending_time - time_margin
    time_range_end = pending_time + time_margin
    matching_success = succes_data[
        (succes_data['TIMESTAMP'] >= time_range_start) &
        (succes_data['TIMESTAMP'] <= time_range_end) &
        (succes_data['FRMSISDN'] == row['FRMSISDN']) &
        (succes_data['AMOUNT'] == row['AMOUNT'])
    ]
    if not matching_success.empty:
        return int(matching_success['FRMSISDN'].values[0])
    else:
        return 'No Match'

# Ajouter la colonne ACTION à airtime_pending en appliquant determine_action
airtime_pending['ACTION'] = airtime_pending.apply(determine_action, axis=1, succes_data=file_succes)

# Ajouter la colonne SUCCES_TIMESTAMP à airtime_pending en appliquant timestamp_succes
airtime_pending['SUCCES_TIMESTAMP'] = airtime_pending.apply(timestamp_succes, axis=1, succes_data=file_succes)

# Ajouter la colonne SUCCES_AMOUNT à airtime_pending en appliquant amount_succes
airtime_pending['SUCCES_AMOUNT'] = airtime_pending.apply(amount_succes, axis=1, succes_data=file_succes)

# Ajouter la colonne SUCCES_FRMSISDN à airtime_pending en appliquant frmsisdn_succes
airtime_pending['SUCCES_FRMSISDN'] = airtime_pending.apply(frmsisdn_succes, axis=1, succes_data=file_succes)

# Affichage du résultat
print(airtime_pending[['REFERENCEID','ACTION','FRMSISDN','AMOUNT','TIMESTAMP','SUCCES_FRMSISDN','SUCCES_AMOUNT','SUCCES_TIMESTAMP']])

# Sélectionner les colonnes finales à inclure dans le fichier de sortie
airtime_pending = airtime_pending[['REFERENCEID','ACTION','FRMSISDN','AMOUNT','TIMESTAMP']]

# Sauvegarder les résultats dans un fichier Excel en ajustant les options d'affichage pour éviter la notation exponentielle
output_file = "D:\\Programe\\airtime_pending_with_action.xlsx"
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    airtime_pending.to_excel(writer, index=False)
    # Récupérer le workbook et la feuille
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']  # Nom de la feuille peut être différent

    # Définir le format pour les colonnes numériques pour éviter la notation exponentielle
    format_num = workbook.add_format({'num_format': '0'})

    # Appliquer le format à toutes les colonnes
    for col_num, value in enumerate(airtime_pending.columns.values):
        worksheet.set_column(col_num, col_num, None, format_num)

# Confirmation de la sauvegarde des résultats
print(f"Résultats sauvegardés dans {output_file}")
