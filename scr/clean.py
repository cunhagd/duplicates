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

# Função para converter data DD/MM/AAAA em objeto datetime
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        return None

# Função para limpar duplicatas
def clean_duplicate_links():
    conn = None
    cursor = None
    try:
        # Conecta ao banco de dados
        conn = connect_db()
        cursor = conn.cursor()

        # Query para encontrar links duplicados e suas informações
        query = """
        SELECT id, link, data, portal, estrategica
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

        # Organiza os resultados por link
        duplicates = {}
        for id, link, data, portal, estrategica in results:
            if link not in duplicates:
                duplicates[link] = []
            duplicates[link].append({
                "id": id,
                "link": link,
                "data": data,
                "data_obj": parse_date(data),  # Converte para datetime
                "portal": portal,
                "estrategica": estrategica if estrategica is not None else False
            })

        # Identifica quais notícias manter e quais excluir
        kept_records = []
        deleted_records = []
        strategic_kept = []
        delete_ids = []

        for link, records in duplicates.items():
            if len(records) <= 1:
                continue  # Ignora links não duplicados

            # Verifica se há alguma notícia estratégica
            strategic_record = next((r for r in records if r["estrategica"]), None)
            if strategic_record:
                # Mantém a notícia estratégica e marca as demais para exclusão
                kept_records.append(strategic_record)
                strategic_kept.append(strategic_record)
                deleted = [r for r in records if r["id"] != strategic_record["id"]]
                deleted_records.extend(deleted)
                delete_ids.extend([r["id"] for r in deleted])
            else:
                # Nenhuma notícia estratégica: mantém a mais antiga
                valid_records = [r for r in records if r["data_obj"] is not None]
                if not valid_records:
                    # Se nenhuma data é válida, mantém a primeira notícia
                    kept_records.append(records[0])
                    deleted = [r for r in records[1:]]
                    deleted_records.extend(deleted)
                    delete_ids.extend([r["id"] for r in deleted])
                else:
                    # Mantém a notícia com a data mais antiga
                    oldest_record = min(valid_records, key=lambda r: r["data_obj"])
                    kept_records.append(oldest_record)
                    deleted = [r for r in records if r["id"] != oldest_record["id"]]
                    deleted_records.extend(deleted)
                    delete_ids.extend([r["id"] for r in deleted])

        # Executa a exclusão das duplicatas
        if delete_ids:
            delete_query = "DELETE FROM noticias WHERE id = ANY(%s);"
            cursor.execute(delete_query, (delete_ids,))
            conn.commit()
            print(f"{len(delete_ids)} notícias duplicadas excluídas.")
        else:
            print("Nenhuma notícia duplicada encontrada para exclusão.")

        # Gera o relatório
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_kept": len(kept_records),
                "total_deleted": len(deleted_records),
                "total_strategic_kept": len(strategic_kept)
            },
            "kept_records": [
                {
                    "id": r["id"],
                    "link": r["link"],
                    "data": r["data"],
                    "portal": r["portal"],
                    "estrategica": r["estrategica"]
                } for r in kept_records
            ],
            "deleted_records": [
                {
                    "id": r["id"],
                    "link": r["link"],
                    "data": r["data"],
                    "portal": r["portal"],
                    "estrategica": r["estrategica"]
                } for r in deleted_records
            ],
            "strategic_kept": [
                {
                    "id": r["id"],
                    "link": r["link"],
                    "data": r["data"],
                    "portal": r["portal"],
                    "estrategica": r["estrategica"]
                } for r in strategic_kept
            ]
        }

        return report

    except Exception as e:
        print(f"Erro ao processar duplicatas: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Função para salvar o relatório em JSON
def save_report(data, filename="clean_duplicate_links_report.json"):
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
        # Limpa duplicatas e gera relatório
        report = clean_duplicate_links()

        # Salva o relatório
        save_report(report)

        # Informa os totais
        print(f"Total de notícias mantidas: {report['summary']['total_kept']}")
        print(f"Total de notícias excluídas: {report['summary']['total_deleted']}")
        print(f"Total de notícias estratégicas preservadas: {report['summary']['total_strategic_kept']}")

    except Exception as e:
        print(f"Erro na execução do script: {e}")

if __name__ == "__main__":
    main()