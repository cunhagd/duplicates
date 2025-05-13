import psycopg2
import json
import os
from datetime import datetime

# Configurações do banco de dados
DB_HOST = "metro.proxy.rlwy.net"
DB_PORT = "30848"
DB_NAME = "railway"
DB_USER = "postgres"
DB_PASSWORD = "HomctJkRyZIGzYhrlmFRdKHZPJJmWylh"
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Função para conectar ao banco de dados
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        raise

# Função para encontrar links duplicados
def find_duplicate_links():
    conn = None
    cursor = None
    try:
        # Conecta ao banco de dados
        conn = connect_db()
        cursor = conn.cursor()

        # Query para encontrar links duplicados e suas informações
        query = """
        SELECT link, data, portal
        FROM noticias
        WHERE link IN (
            SELECT link
            FROM noticias
            GROUP BY link
            HAVING COUNT(*) > 1
        )
        ORDER BY link, data;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Organiza os resultados em um dicionário por link
        duplicates = {}
        for link, data, portal in results:
            if link not in duplicates:
                duplicates[link] = []
            # Mantém a data no formato DD/MM/AAAA (já está como string no banco)
            data_str = data if data else None
            duplicates[link].append({
                "data": data_str,
                "portal": portal
            })

        # Filtra apenas links com mais de uma ocorrência (por segurança)
        duplicate_report = {
            "generated_at": datetime.now().isoformat(),
            "duplicates": [
                {"link": link, "occurrences": occurrences}
                for link, occurrences in duplicates.items()
                if len(occurrences) > 1
            ]
        }

        return duplicate_report

    except Exception as e:
        print(f"Erro ao consultar o banco de dados: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Função para salvar o relatório em JSON
def save_report(data, filename="duplicate_links_report.json"):
    filepath = os.path.join(os.getcwd(), filename)
    try:
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print(f"Relatório salvo em: {filepath}")
    except Exception as e:
        print(f"Erro ao salvar o relatório: {e}")
        raise

# Função principal
def main():
    try:
        # Encontra links duplicados
        duplicate_report = find_duplicate_links()

        # Salva o relatório
        save_report(duplicate_report)

        # Informa o número de links duplicados encontrados
        num_duplicates = len(duplicate_report["duplicates"])
        print(f"Total de links duplicados encontrados: {num_duplicates}")

    except Exception as e:
        print(f"Erro na execução do script: {e}")

if __name__ == "__main__":
    main()