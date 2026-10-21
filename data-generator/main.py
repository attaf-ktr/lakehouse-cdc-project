import psycopg2, time, random
from faker import Faker

fake = Faker()
conn = psycopg2.connect(dbname="lakehouse", user="postgres", password="password", host="localhost", port="5432")
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS clients (id SERIAL PRIMARY KEY, nom VARCHAR(100), email VARCHAR(100), statut VARCHAR(20));")

client_ids = []
print("Génération en cours...")
try:
    while True:
        action = random.choices(['INSERT', 'UPDATE', 'DELETE'], weights=[70, 20, 10])[0]
        if action == 'INSERT':
            cur.execute("INSERT INTO clients (nom, email, statut) VALUES (%s, %s, %s) RETURNING id", (fake.name(), fake.email(), 'ACTIF'))
            client_ids.append(cur.fetchone()[0])
        elif action == 'UPDATE' and client_ids:
            cur.execute("UPDATE clients SET statut = %s WHERE id = %s", ('INACTIF', random.choice(client_ids)))
        elif action == 'DELETE' and client_ids:
            cid = random.choice(client_ids)
            cur.execute("DELETE FROM clients WHERE id = %s", (cid,))
            client_ids.remove(cid)
        time.sleep(1)
except KeyboardInterrupt:
    pass