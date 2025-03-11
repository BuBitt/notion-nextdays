import os
import re
import json
import time
import glob
import asyncio
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from rich.console import Console
from colorlog import ColoredFormatter
from notion_client import AsyncClient
from logging.handlers import RotatingFileHandler

# Configurações iniciais
console = Console()
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, "logs")
caches_dir = os.path.join(current_dir, "caches")

# Criar os diretórios antes de configurar o logger
os.makedirs(logs_dir, exist_ok=True)
os.makedirs(caches_dir, exist_ok=True)

# Configuração do logger
log_file = os.path.join(
    logs_dir, f"notion_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
handler.setFormatter(log_formatter)

color_formatter = ColoredFormatter(
    "%(bold)s%(asctime)s%(reset)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
    log_colors={
        "DEBUG": "purple",
        "INFO": "blue",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    },
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)

logger = logging.getLogger("NotionSyncLogger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.addHandler(console_handler)

# Arquivos de cache
PAGE_CACHE_FILE = os.path.join(caches_dir, "page_cache.json")
MATERIA_CACHE_FILE = os.path.join(caches_dir, "materia_cache.json")
LAST_MESSAGE_FILE = os.path.join(caches_dir, "last_message.json")

# Carregar variáveis de ambiente
load_dotenv()
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID_WPP = os.getenv("TELEGRAM_CHAT_ID_WPP")

# Cliente Notion assíncrono
notion = AsyncClient(auth=NOTION_API_KEY)


# Funções utilitárias de cache
def check_and_update_cache(file_path, cache_name, max_age_days=1):
    if os.path.exists(file_path):
        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if (datetime.now() - mod_time).days > max_age_days:
            logger.info(
                f"Cache {cache_name} desatualizado (>{max_age_days} dia). Limpando."
            )
            return {}
    return load_cache(file_path, cache_name)


def load_cache(file_path, cache_name):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                cache = json.load(f)
                logger.info(
                    f"Cache {cache_name} carregado de {file_path} com {len(cache)} itens"
                )
                return cache
        except Exception as e:
            logger.error(f"Erro ao carregar cache {cache_name} de {file_path}: {e}")
            return {}
    return {}


def save_cache(cache, file_path, cache_name):
    try:
        with open(file_path, "w") as f:
            json.dump(cache, f)
        logger.info(f"Cache {cache_name} salvo em {file_path} com {len(cache)} itens")
    except Exception as e:
        logger.error(f"Erro ao salvar cache {cache_name} em {file_path}: {e}")


# Função para limpar logs antigos
def clean_old_logs(max_age_days=7):
    log_files = glob.glob("logs/notion_sync_*.log")
    current_time = time.time()
    for log_file in log_files:
        file_time = os.path.getmtime(log_file)
        if (current_time - file_time) > (max_age_days * 24 * 60 * 60):
            try:
                os.remove(log_file)
                logger.info(f"Arquivo de log antigo removido: {log_file}")
            except Exception as e:
                logger.error(f"Erro ao remover log {log_file}: {e}")


# Funções de interação com a API do Notion
async def check_notion_api():
    try:
        await notion.databases.retrieve(database_id=NOTION_DATABASE_ID)
        logger.info("API do Notion está funcionando corretamente.")
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar à API do Notion: {e}")
        return False


async def fetch_notion_data(database_id):
    all_results = []
    try:
        response = await notion.databases.query(database_id=database_id)
        all_results.extend(response["results"])
        while response.get("has_more"):
            response = await notion.databases.query(
                database_id=database_id, start_cursor=response["next_cursor"]
            )
            all_results.extend(response["results"])
        logger.info(f"Dados obtidos do Notion: {len(all_results)} itens")
        return all_results
    except Exception as e:
        logger.error(f"Erro ao buscar dados do Notion: {e}")
        raise


async def get_notion_page(page_id, cache):
    if page_id in cache:
        logger.debug(f"Cache hit para página {page_id}")
        return cache[page_id]
    try:
        page = await notion.pages.retrieve(page_id=page_id)
        cache[page_id] = page
        logger.debug(f"Página {page_id} carregada e adicionada ao cache")
        return page
    except Exception as e:
        logger.warning(f"Falha ao buscar página {page_id}: {e}")
        return {}


# Funções de extração de dados do Notion
def extract_title(props, prop_name):
    try:
        return (
            props.get(prop_name, {}).get("title", [{}])[0].get("plain_text", "").strip()
        )
    except (IndexError, AttributeError):
        logger.debug(f"Erro ao extrair título de '{prop_name}'")
        return ""


def extract_select(props, prop_name):
    return props.get(prop_name, {}).get("select", {}).get("name", "") or ""


async def extract_relation_titles(props, prop_name, cache):
    relations = props.get(prop_name, {}).get("relation", [])
    if not relations:
        return ""
    titles = []
    for rel in relations:
        rel_id = rel["id"]
        if rel_id in cache:
            logger.debug(f"Cache hit para relação {rel_id}")
            titles.append(cache[rel_id])
        else:
            try:
                page_data = await get_notion_page(rel_id, cache)
                title = extract_title(page_data.get("properties", {}), "Name")
                if title:
                    cache[rel_id] = title
                    logger.debug(f"Relação {rel_id} cached: {title}")
                    titles.append(title)
            except Exception as e:
                logger.debug(f"Erro ao processar relação {rel_id}: {e}")
    return ", ".join(titles) or "Nenhuma relação encontrada"


def extract_date(props, prop_name):
    if props is None:
        logger.debug(f"Propriedades ausentes ao tentar extrair '{prop_name}'")
        return ""
    return props.get(prop_name, {}).get("date", {}).get("start", "")


def extract_rich_text(props, prop_name):
    rich_text = props.get(prop_name, {}).get("rich_text", [])
    return rich_text[0].get("text", {}).get("content", "") if rich_text else ""


# Funções de processamento de dados
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)


def calculate_days_remaining(entrega_date):
    if not entrega_date:
        return None
    try:
        entrega_dt = datetime.fromisoformat(entrega_date)
        return (entrega_dt - today).days
    except ValueError as e:
        logger.error(f"Erro ao calcular dias restantes para '{entrega_date}': {e}")
        return None


async def process_result(result, page_cache, materia_cache):
    props = result.get("properties")
    if props is None:
        logger.error(
            f"Resultado inválido do Notion: 'properties' é None para {result.get('id', 'ID desconhecido')}"
        )
        return {
            "Professor": "",
            "Status": "",
            "Tipo": "",
            "Estágio": "",
            "Matéria": "",
            "Entrega": "",
            "Dias Restantes": None,
            "Descrição": "",
            "Tópicos": "",
        }
    entrega_date = extract_date(props, "Data de Entrega")
    return {
        "Professor": extract_title(props, "Professor"),
        "Status": extract_select(props, "Status"),
        "Tipo": extract_select(props, "Tipo"),
        "Estágio": extract_select(props, "Estágio"),
        "Matéria": await extract_relation_titles(props, "Matéria", materia_cache),
        "Entrega": entrega_date,
        "Dias Restantes": calculate_days_remaining(entrega_date),
        "Descrição": extract_rich_text(props, "Descrição"),
        "Tópicos": await extract_relation_titles(props, "Tópicos", page_cache),
    }


async def process_batch(batch, page_cache, materia_cache):
    tasks = [process_result(result, page_cache, materia_cache) for result in batch]
    return await asyncio.gather(*tasks)


# Funções de formatação de mensagem
def formatar_data(data_str):
    meses = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    data = datetime.strptime(data_str, "%Y-%m-%d")
    dia = data.day
    mes = meses[data.month]
    return f"{dia} de {mes}"


def escapar_markdown_v2(texto):
    caracteres_reservados = [
        "_",
        "*",
        "[",
        "]",
        "(",
        ")",
        "~",
        "`",
        ">",
        "#",
        "+",
        "-",
        "=",
        "|",
        "{",
        "}",
        ".",
        "!",
    ]
    for char in caracteres_reservados:
        texto = texto.replace(char, f"\\{char}")
    return texto


def gerar_mensagem_tarefa(tarefa):
    dias_restantes = tarefa.get("Dias Restantes")
    if dias_restantes > 7:
        return None

    tipo = tarefa.get("Tipo", "N/D").upper()
    materia = tarefa.get("Matéria", "N/D")
    entrega = tarefa.get("Entrega", "N/D")
    descricao = tarefa.get("Descrição") or "Sem descrição"
    data_formatada = formatar_data(entrega) if entrega != "N/D" else "N/D"
    tipo = escapar_markdown_v2(tipo)
    materia = escapar_markdown_v2(materia)
    descricao = escapar_markdown_v2(descricao)
    topicos = tarefa.get("Tópicos") or "Sem Tópicos"

    topicos_formatados = "\n".join(
        [
            f"\\- _{escapar_markdown_v2(topico.strip())}_"
            for topico in topicos.split(", ")
        ]
    )

    dias_texto = (
        "🚨 HOJE 🚨"
        if dias_restantes == 0
        else f"{dias_restantes} DIA{'S' if dias_restantes > 1 else ''}"
    )

    mensagem = (
        f"*{tipo} \\- {materia}*\n"
        f"Dias Restantes: *{dias_texto}*\n"
        f"Entrega: `{data_formatada}`\n"
        f"Tópicos:\n{topicos_formatados}\n"
        f"Descrição: _{descricao}_"
    )
    return mensagem


def print_whatsapp_markdown(mensagem):
    return re.sub(r"\\(.)", r"\1", mensagem)


# Funções de interação com Telegram
def delete_previous_message(chat_id, message_id):
    import requests

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
    payload = {"chat_id": chat_id, "message_id": message_id}
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logger.info(f"Mensagem anterior (ID: {message_id}) apagada com sucesso!")
        else:
            logger.error(
                f"Erro ao apagar mensagem: {response.status_code} - {response.text}"
            )
    except requests.RequestException as e:
        logger.error(f"Erro de conexão ao tentar apagar mensagem: {e}")


def enviar_mensagem_telegram(mensagem, t_chat_id=TELEGRAM_CHAT_ID, parse_mode=None):
    import requests

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": t_chat_id, "text": mensagem}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logger.info(
                f"Mensagem enviada ao Telegram (chat id {t_chat_id}) com sucesso!"
            )
            return response.json()["result"]["message_id"]
        logger.error(
            f"chat id {t_chat_id} - Erro ao enviar mensagem: {response.status_code} - {response.text}"
        )
        return None
    except requests.RequestException as e:
        logger.error(f"chat id {t_chat_id} - Erro de conexão com o Telegram: {e}")
        return None


# Função principal ajustada para remover Polars
async def main():
    logger.info("Iniciando programa e checando API do Notion...")
    if not all([NOTION_API_KEY, NOTION_DATABASE_ID]):
        logger.error(
            "Variáveis de ambiente 'NOTION_API_KEY' e 'NOTION_DATABASE_ID' não definidas!"
        )
        raise ValueError("Variáveis de ambiente não definidas!")
    if not await check_notion_api():
        logger.error("Checagem da API falhou. Encerrando programa.")
        raise SystemExit("Erro na API do Notion")
    logger.info("Variáveis de ambiente carregadas e API validada com sucesso!")

    # Carregar caches
    page_cache = check_and_update_cache(PAGE_CACHE_FILE, "page_cache", max_age_days=3)
    materia_cache = check_and_update_cache(
        MATERIA_CACHE_FILE, "materia_cache", max_age_days=3
    )
    last_message_info = load_cache(LAST_MESSAGE_FILE, "last_message")

    # Obter e processar dados
    logger.info("Iniciando requisição ao Notion...")
    results = await fetch_notion_data(NOTION_DATABASE_ID)
    logger.info("Dados obtidos com sucesso! Processando...")

    batch_size = 50
    batches = [results[i : i + batch_size] for i in range(0, len(results), batch_size)]
    all_rows = []

    for batch in batches:
        processed_batch = await process_batch(batch, page_cache, materia_cache)
        all_rows.extend(processed_batch)

    logger.info("Processamento concluído! Filtrando e ordenando dados...")

    # Filtrar e ordenar sem Polars
    filtered_rows = []
    for row in all_rows:
        dias_restantes = row["Dias Restantes"]
        status = row["Status"]
        # Filtro: exclui "Concluído" e mantém apenas tarefas de 0 a 7 dias restantes
        if (
            status != "Concluído"
            and dias_restantes is not None
            and 0 <= dias_restantes <= 7
        ):
            filtered_rows.append(row)

    # Ordenar por "Dias Restantes" (nulls_last=True)
    filtered_rows.sort(key=lambda x: (x["Dias Restantes"] is None, x["Dias Restantes"]))

    # Salvar caches e limpar logs
    clean_old_logs(max_age_days=7)
    save_cache(page_cache, PAGE_CACHE_FILE, "page_cache")
    save_cache(materia_cache, MATERIA_CACHE_FILE, "materia_cache")

    # Gerar e enviar mensagens
    tarefas = filtered_rows  # Já é uma lista de dicionários
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_CHAT_ID_WPP]):
        raise ValueError(
            "Variáveis 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID' e 'TELEGRAM_CHAT_ID_WPP' não definidas!"
        )

    mensagens = [gerar_mensagem_tarefa(tarefa) for tarefa in tarefas]
    mensagens_validas = [msg for msg in mensagens if msg is not None]

    if mensagens_validas:
        separador = "\n\n*\\-\\-\\-\\-\\-\\-*\n\n"
        mensagem_conjunta = separador.join(mensagens_validas)
        current_date = datetime.now().strftime("%Y-%m-%d")
        if (
            last_message_info.get("date") == current_date
            and "message_id" in last_message_info
        ):
            delete_previous_message(TELEGRAM_CHAT_ID, last_message_info["message_id"])

        message_id = enviar_mensagem_telegram(
            mensagem_conjunta, parse_mode="MarkdownV2"
        )
        if message_id:
            last_message_info = {"message_id": message_id, "date": current_date}
            save_cache(last_message_info, LAST_MESSAGE_FILE, "last_message")

        mensagem_wpp_bc = f"```md\n{print_whatsapp_markdown(mensagem_conjunta)}```"
        enviar_mensagem_telegram(
            mensagem_wpp_bc, TELEGRAM_CHAT_ID_WPP, parse_mode="Markdown"
        )
        logger.debug(print_whatsapp_markdown(mensagem_conjunta))
        logger.info("Mensagem compatível com WhatsApp enviada!")
    else:
        logger.info(
            "Nenhuma tarefa com Dias Restantes entre 0 e 7 e Status diferente de 'Concluído' encontrada."
        )


if __name__ == "__main__":
    asyncio.run(main())
