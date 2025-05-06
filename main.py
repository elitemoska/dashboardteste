# main.py
# coding: utf-8
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import threading
import os

from flask_socketio import SocketIO

from src.api.routes import app  # Importa o app Flask de routes.py
from src.config.database import get_db, init_db  # Importa o gerador get_db
from src.models.client import Client  # Importa o modelo Client
from src.services.yeastar_service import atualizar_dados, carregar_dados_do_json
from src.services.zabbix_service import collect_zabbix_data, save_zabbix_data
from src.utils.logging import configurar_logging  # Assume que este utilitário existe
from src.config.settings import INTERVALO_ATUALIZACAO, PORTA_SOCKETIO  # Usa porta específica para SocketIO

# Inicializa o SocketIO com o app Flask
# Ajuste cors_allowed_origins para produção
socketio = SocketIO(app, cors_allowed_origins="*")

# Cria a pasta uploads se não existir
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def executar_atualizacao_cliente(nome_cliente):
    """Executa a atualização para um cliente específico."""
    logger = configurar_logging(nome_cliente)  # Configura logger específico
    configuracao_cliente = None
    cliente_encontrado = False

    # Busca configuração do cliente no DB
    with get_db() as db:
        cliente = db.query(Client).filter(Client.nome == nome_cliente).first()
        if cliente:
            cliente_encontrado = True
            # Copia dados necessários
            configuracao_cliente = {
                "nome": cliente.nome,
                "base_url": cliente.base_url,
                "client_id": cliente.client_id,
                "client_secret": cliente.client_secret,
            }
        else:
            logger.error(f"Cliente {nome_cliente} não encontrado no banco de dados para atualização.")

    # Executa a atualização fora do 'with' se o cliente foi encontrado
    if cliente_encontrado and configuracao_cliente:
        try:
            # Passa o logger configurado
            return atualizar_dados(nome_cliente, configuracao_cliente, logger)
        except Exception as e:
            logger.exception(f"Erro não tratado em atualizar_dados para {nome_cliente}: {e}")
            return False, f"Erro interno ao processar {nome_cliente}: {e}"
    elif not cliente_encontrado:
        return False, f"Cliente {nome_cliente} não encontrado."
    else:
        # Caso onde cliente foi encontrado mas configuracao_cliente é None (improvável)
        logger.error(f"Configuração para cliente {nome_cliente} não pôde ser lida.")
        return False, f"Erro ao ler configuração do cliente {nome_cliente}."

def loop_atualizacao():
    """Loop principal para atualizar dados de todos os clientes em paralelo."""
    root_logger = logging.getLogger("main_loop")  # Logger específico para o loop
    root_logger.info("Loop de atualização iniciado.")
    while True:
        start_time = time.time()
        resultados = {}
        erros = {}
        clientes_para_processar = []

        # 1. Obter lista de clientes do DB
        try:
            with get_db() as db:
                clientes_db = db.query(Client).all()
                if not clientes_db:
                    root_logger.warning("Nenhum cliente encontrado no banco de dados. Aguardando...")
                    time.sleep(INTERVALO_ATUALIZACAO)
                    continue
                # Copia apenas os nomes para usar fora do 'with'
                clientes_para_processar = [c.nome for c in clientes_db]
                root_logger.info(f"Encontrados {len(clientes_para_processar)} clientes para processar.")
        except Exception as e:
            root_logger.exception(f"Erro ao buscar lista de clientes do DB: {e}")
            time.sleep(INTERVALO_ATUALIZACAO)  # Espera antes de tentar novamente
            continue

        # 2. Coleta Yeastar em paralelo
        if clientes_para_processar:
            with ThreadPoolExecutor(max_workers=min(len(clientes_para_processar), 10)) as executor:
                # Submete usando apenas o nome do cliente
                futures = {
                    executor.submit(executar_atualizacao_cliente, nome_cliente): nome_cliente
                    for nome_cliente in clientes_para_processar
                }
                for future in futures:
                    nome_cliente = futures[future]
                    try:
                        sucesso, mensagem = future.result()
                        resultados[nome_cliente] = sucesso
                        erros[nome_cliente] = mensagem if not sucesso else None
                        if sucesso:
                            # Carrega dados atualizados e emite via socket
                            # Assumindo que carregar_dados_do_json não precisa do DB
                            dados = carregar_dados_do_json(nome_cliente)
                            if dados:
                                socketio.emit('update', {'client': nome_cliente, 'data': dados[0]})
                                root_logger.debug(f"Update emitido para {nome_cliente} via SocketIO.")
                            else:
                                root_logger.warning(f"Dados JSON não encontrados para {nome_cliente} após atualização bem sucedida.")
                        else:
                            root_logger.error(f"Falha na atualização Yeastar para {nome_cliente}: {mensagem}")
                    except Exception as e:
                        root_logger.exception(f"Erro no future da atualização Yeastar para {nome_cliente}: {e}")
                        resultados[nome_cliente] = False
                        erros[nome_cliente] = f"Erro interno no future: {str(e)}"

        # 3. Coleta Zabbix (execução sequencial após Yeastar)
        try:
            zabbix_data_collected = {}
            with get_db() as db:  # Nova sessão para operações Zabbix
                # Passa o DB para as funções que precisam dele
                zabbix_data = collect_zabbix_data(db)  # Assume que retorna um dict {client_name: data}
                if zabbix_data:
                    save_zabbix_data(db, zabbix_data)  # Salva os dados coletados
                    zabbix_data_collected = zabbix_data  # Guarda para emitir fora do with
                    root_logger.info("Dados do Zabbix coletados e salvos com sucesso.")
                else:
                    root_logger.info("Nenhum dado do Zabbix foi coletado nesta execução.")

            # Emite updates Zabbix fora do 'with'
            for client_name, data in zabbix_data_collected.items():
                # Recarrega dados do JSON para incluir Zabbix e Yeastar
                # (Assume que save_zabbix_data atualiza o mesmo JSON ou que carregar_dados combina as fontes)
                dados_combinados = carregar_dados_do_json(client_name)
                if dados_combinados:
                    socketio.emit('update', {'client': client_name, 'data': dados_combinados[0]})
                    root_logger.debug(f"Update (com Zabbix) emitido para {client_name} via SocketIO.")
                else:
                    root_logger.warning(f"Dados JSON não encontrados para {client_name} após coleta Zabbix.")

        except Exception as e:
            root_logger.exception(f"Erro durante a coleta/salvamento/emissão de dados do Zabbix: {e}")

        # 4. Log de resumo
        sucessos = sum(1 for r in resultados.values() if r)
        falhas = len(clientes_para_processar) - sucessos  # Baseado nos clientes que tentamos processar
        mensagem_resumo = f"Ciclo de atualização Yeastar concluído: Sucessos={sucessos}, Falhas={falhas}. "
        if falhas > 0:
            falhas_detalhes = [f"{cliente}: {erros[cliente]}" for cliente, sucesso in resultados.items() if not sucesso]
            mensagem_resumo += "Detalhes das falhas: " + "; ".join(falhas_detalhes)
        root_logger.info(mensagem_resumo)

        # 5. Intervalo dinâmico
        elapsed_time = time.time() - start_time
        sleep_time = max(0, INTERVALO_ATUALIZACAO - elapsed_time)
        root_logger.info(f"Tempo do ciclo: {elapsed_time:.2f}s. Próxima atualização em {sleep_time:.2f}s")
        time.sleep(sleep_time)

def main():
    """Função principal para iniciar o programa."""
    # Configuração básica do logging (pode ser sobrescrita por configurar_logging)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    root_logger = logging.getLogger("main_startup")
    root_logger.info("Iniciando aplicação...")

    # Inicializa o banco de dados
    try:
        init_db()
        root_logger.info("Banco de dados inicializado com sucesso.")
    except Exception as e:
        root_logger.exception(f"Falha crítica ao inicializar o banco de dados: {e}")
        return  # Aborta a execução se o DB falhar

    # Inicia o loop de atualização em uma thread separada
    update_thread = threading.Thread(target=loop_atualizacao, name="UpdateLoopThread")
    update_thread.daemon = True  # Permite que o programa saia mesmo se a thread estiver rodando
    update_thread.start()
    root_logger.info("Loop de atualização iniciado em thread separada.")

    # Inicia o servidor Flask-SocketIO
    # Use a porta definida em settings ou um padrão
    host = "0.0.0.0"
    port = PORTA_SOCKETIO  # Use a porta configurada
    root_logger.info(f"Iniciando servidor Flask-SocketIO em {host}:{port}")
    # debug=False para produção. use_reloader=False é recomendado quando se usa threads customizadas.
    socketio.run(app, host=host, port=port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)

if __name__ == "__main__":
    main()