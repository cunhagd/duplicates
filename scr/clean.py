import psycopg2
import json
import os
from datetime import datetime

# Obtém a string de conexão do ambiente
DB_URL = os.getenv("DB_URL")

# Nível de debug: definido como False para não mostrar logs detalhados em produção
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

# Função para limpar duplicatas
def clean_duplicate_links():
    conn = None
    cursor = None
    try:
        conn = connect_db()
        cursor = conn.cursor()

        log_info("Iniciando processo de limpeza de links duplicados.")

        # --- Passo 1: Excluir links que já existem na lixeira ---
        log_info("Verificando e excluindo links que já existem na tabela 'lixeira'...")
        query_lixeira = """
            SELECT id, link FROM noticias
            WHERE link IN (SELECT link FROM lixeira);
        """
        cursor.execute(query_lixeira)
        results_lixeira = cursor.fetchall()

        delete_ids_lixeira = [id for id, _ in results_lixeira]
        deleted_by_lixeira = len(delete_ids_lixeira)

        if delete_ids_lixeira:
            delete_query = "DELETE FROM noticias WHERE id = ANY(%s);"
            cursor.execute(delete_query, (delete_ids_lixeira,))
            conn.commit()
            log_info(f"{deleted_by_lixeira} notícias excluídas por já existirem na lixeira.")
        else:
            log_info("Nenhuma notícia foi excluída por conflito com a tabela 'lixeira'.")

        # --- Passo 2: Verificar duplicatas internas na tabela noticias ---
        log_info("Buscando duplicatas internas na tabela 'noticias'...")
        query_duplicates = """
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
        cursor.execute(query_duplicates)
        results_duplicates = cursor.fetchall()

        duplicates = {}
        for id, link, data, portal, estrategica in results_duplicates:
            if link not in duplicates:
                duplicates[link] = []
            duplicates[link].append({
                "id": id,
                "link": link,
                "data": data,
                "data_obj": parse_date(data),
                "portal": portal,
                "estrategica": estrategica if estrategica is not None else False
            })

        # --- Processar duplicatas internas ---
        kept_records = []
        deleted_records = []
        strategic_kept_count = 0
        deleted_by_noticias = 0
        delete_ids = []

        for link, records in duplicates.items():
            strategic_record = next((r for r in records if r["estrategica"]), None)

            if strategic_record:
                strategic_kept_count += 1
                deleted = [r for r in records if r["id"] != strategic_record["id"]]
                kept_records.append(strategic_record)
                deleted_records.extend(deleted)
                delete_ids.extend([r["id"] for r in deleted])
                deleted_by_noticias += len(deleted)
            else:
                valid_records = [r for r in records if r["data_obj"] is not None]
                if not valid_records:
                    kept_records.append(records[-1])  # Keep the last record if no valid dates
                    deleted = records[:-1]
                else:
                    newest_record = max(valid_records, key=lambda r: r["data_obj"])  # Keep the most recent
                    kept_records.append(newest_record)
                    deleted = [r for r in records if r["id"] != newest_record["id"]]

                deleted_records.extend(deleted)
                delete_ids.extend([r["id"] for r in deleted])
                deleted_by_noticias += len(deleted)

        if delete_ids:
            delete_query = "DELETE FROM noticias WHERE id = ANY(%s);"
            cursor.execute(delete_query, (delete_ids,))
            conn.commit()
            log_info(f"{len(delete_ids)} notícias duplicadas excluídas dentro da tabela 'noticias'.")
        else:
            log_info("Nenhuma duplicata interna encontrada na tabela 'noticias'.")

        # --- Preparar relatório final ---
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_deleted_from_lixeira": deleted_by_lixeira,
                "total_deleted_internal_duplicates": len(delete_ids),
                "total_deleted": deleted_by_lixeira + len(delete_ids),
                "total_kept": len(kept_records),
                "total_strategic_kept": strategic_kept_count,
                "deleted_by_lixeira": deleted_by_lixeira,
                "deleted_by_noticias": deleted_by_noticias
            },
            "deleted_from_lixeira": [{"id": id, "link": link} for id, link in results_lixeira],
            "kept_records": [
                {"id": r["id"], "link": r["link"], "data": r["data"], "portal": r["portal"], "estrategica": r["estrategica"]}
                for r in kept_records
            ],
            "deleted_records": [
                {"id": r["id"], "link": r["link"], "data": r["data"], "portal": r["portal"], "estrategica": r["estrategica"]}
                for r in deleted_records
            ]
        }

        # --- Logs finais detalhados ---
        log_info("\n📊 Resumo final:")
        log_info(f"Total de notícias mantidas: {report['summary']['total_kept']}")
        log_info(f"Total de notícias excluídas: {report['summary']['total_deleted']}")
        log_info(f"- Excluídas por existirem na lixeira: {report['summary']['total_deleted_from_lixeira']}")
        log_info(f"- Excluídas por duplicação interna: {report['summary']['total_deleted_internal_duplicates']}")
        log_info(f"Total de notícias estratégicas preservadas: {report['summary']['total_strategic_kept']}")

        return report

    except Exception as e:
        log_warning(f"Erro ao processar duplicatas: {e}")
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
        log_info(f"Relatório salvo em: {filepath}")
    except Exception as e:
        log_warning(f"Erro ao salvar o relatório: {e}")
        raise

# Função principal
def main():
    try:
        # Limpa duplicatas e gera relatório
        report = clean_duplicate_links()

        # Salva o relatório
        save_report(report)

    except Exception as e:
        log_warning(f"Erro na execução do script: {e}")

if __name__ == "__main__":
    main()