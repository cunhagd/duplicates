import sys
import os

# Adiciona o diretório 'scr' ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'scr')))

from scr.find import main as find_duplicate_links
from scr.clean import main as clean_duplicate_links

def main():
    print("Iniciando o processo de verificação e limpeza de duplicatas...")
    
    # Passo 1: Executa o módulo de encontrar duplicatas
    print("\n--- Executando módulo de busca de duplicatas ---")
    try:
        find_duplicate_links()
    except Exception as e:
        print(f"Erro ao executar o módulo de busca de duplicatas: {e}")
        sys.exit(1)
    
    # Passo 2: Executa o módulo de limpeza de duplicatas
    print("\n--- Executando módulo de limpeza de duplicatas ---")
    try:
        clean_duplicate_links()
    except Exception as e:
        print(f"Erro ao executar o módulo de limpeza de duplicatas: {e}")
        sys.exit(1)
    
    print("\nProcesso concluído com sucesso!")

if __name__ == "__main__":
    main()