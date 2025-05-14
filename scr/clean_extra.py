import psycopg2
import json
import os
from datetime import datetime

# Obtém a string de conexão do ambiente
DB_URL = os.getenv("DB_URL")

DEBUG_MODE = True

def log_debug(message):
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

def log_info(message):
    print(f"[INFO] {message}")

def log_warning(message):
    print(f"[WARNING] {message}")

# Função para conectar ao banco de dados
def connect_db():
    try:
        if not DB_URL:
            raise ValueError("A variável de ambiente DB_URL não está definida")
        conn = psycopg2.connect(DB_URL)
        log_info("Conexão com o banco de dados estabelecida.")
        return conn
    except Exception as e:
        log_warning(f"Erro ao conectar ao banco de dados: {e}")
        raise

# Função para converter data DD/MM/AAAA em objeto datetime
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        return None

# Função principal de limpeza extra
def clean_extra_duplicates():
    conn = None
    cursor = None
    try:
        conn = connect_db()
        cursor = conn.cursor()

        log_info("Iniciando processo de limpeza extra de duplicatas por título e portal.")

        # Busca todos os registros com título e portal duplicados
        query_duplicates = """
            SELECT id, titulo, portal, relevancia, data
            FROM noticias
            WHERE (titulo, portal) IN (
                SELECT titulo, portal
                FROM noticias
                GROUP BY titulo, portal
                HAVING COUNT(*) > 1
            )
            ORDER BY titulo, portal;
        """

        cursor.execute(query_duplicates)
        results_duplicates = cursor.fetchall()

        duplicates_by_title_portal = {}

        for row in results_duplicates:
            id, titulo, portal, relevancia, data = row
            key = (titulo, portal)

            if key not in duplicates_by_title_portal:
                duplicates_by_title_portal[key] = []

            duplicates_by_title_portal[key].append({
                "id": id,
                "titulo": titulo,
                "portal": portal,
                "relevancia": relevancia,
                "data": data,
                "data_obj": parse_date(data),
            })

        kept_records = []
        deleted_records = []
        delete_ids = []

        for key, records in duplicates_by_title_portal.items():
            # Filtra registros com relevância definida
            relevant_records = [r for r in records if r["relevancia"] is not None and r["relevancia"].strip() != ""]

            if len(relevant_records) >= 1:
                # Mantém o primeiro com relevância
                keep_record = relevant_records[0]
                deleted = [r for r in records if r["id"] != keep_record["id"]]
            else:
                # Mantém o mais antigo pela data
                valid_records = [r for r in records if r["data_obj"] is not None]
                if valid_records:
                    keep_record = min(valid_records, key=lambda x: x["data_obj"])
                else:
                    # Sem data válida, mantém o primeiro encontrado
                    keep_record = records[0]
                deleted = [r for r in records if r["id"] != keep_record["id"]]

            kept_records.append(keep_record)
            deleted_records.extend(deleted)
            delete_ids.extend([r["id"] for r in deleted])

        # Realiza exclusões
        if delete_ids:
            delete_query = "DELETE FROM noticias WHERE id = ANY(%s);"
            cursor.execute(delete_query, (delete_ids,))
            conn.commit()
            log_info(f"{len(delete_ids)} notícias duplicadas excluídas com base em título e portal.")

        else:
            log_info("Nenhuma duplicata encontrada com base em título e portal.")

        # Gera relatório
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_duplicates_groups": len(duplicates_by_title_portal),
                "total_deleted": len(delete_ids),
                "total_kept": len(kept_records),
            },
            "deleted_records": [
                {"id": r["id"], "titulo": r["titulo"], "portal": r["portal"], "relevancia": r["relevancia"], "data": r["data"]}
                for r in deleted_records
            ],
            "kept_records": [
                {"id": r["id"], "titulo": r["titulo"], "portal": r["portal"], "relevancia": r["relevancia"], "data": r["data"]}
                for r in kept_records
            ]
        }

        log_info("\n📊 Resumo da limpeza extra:")
        log_info(f"Grupos de duplicatas encontrados: {report['summary']['total_duplicates_groups']}")
        log_info(f"Notícias excluídas: {report['summary']['total_deleted']}")
        log_info(f"Notícias mantidas: {report['summary']['total_kept']}")

        return report

    except Exception as e:
        log_warning(f"Erro durante a execução da limpeza extra: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Salva o relatório em JSON
def save_report(data, filename="clean_extra.json"):
    filepath = os.path.join(os.getcwd(), filename)
    try:
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        log_info(f"Relatório salvo em: {filepath}")
    except Exception as e:
        log_warning(f"Erro ao salvar o relatório: {e}")
        raise

# Função principal
def main():
    try:
        log_info("Iniciando módulo de limpeza extra...")
        report = clean_extra_duplicates()
        save_report(report)
        log_info("Processo de limpeza extra concluído com sucesso!")
    except Exception as e:
        log_warning(f"Erro na execução do script: {e}")

if __name__ == "__main__":
    main()